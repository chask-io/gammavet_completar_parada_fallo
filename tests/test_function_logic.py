import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backend.function_logic import FunctionBackend  # noqa: E402
from backend.conductor_common import TENANT_FAIL_CURRENT_PATH  # noqa: E402
from chask_foundation.backend.models import OrchestrationEvent  # noqa: E402


EVENT_ID = "11111111-2222-4333-8444-555555555555"
ROUTE_STOP_ID = "aaaaaaaa-1111-4111-8111-111111111111"
PICKUP_ORDER_ID = "cccccccc-3333-4333-8333-333333333333"
STALE_ROUTE_STOP_ID = "99999999-1111-4111-8111-111111111111"


def _event(args=None):
    return OrchestrationEvent.model_validate(
        {
            "event_id": EVENT_ID,
            "event_type": "function_call",
            "branch": "test",
            "organization_customer_id": None,
            "customer": None,
            "connection_key": "test",
            "organization": {
                "organization_id": "99999999-aaaa-4bbb-8ccc-dddddddddddd",
                "organization_name": "Chask Dev",
            },
            "prompt": "",
            "pipeline_id": 27023,
            "orchestration_session_uuid": "ticket-1",
            "internal_orchestration_session_uuid": None,
            "channel_id": None,
            "entry_point_channel": "whatsapp",
            "source": "agent",
            "target": "function",
            "plan": None,
            "extra_params": {
                "user_phone_number": "+56 9 1111 2222",
                "agent_phone_number": "1051240901403291",
                "tool_calls": [{"args": args or {}}],
            },
            "access_token": "access-token",
            "target_agent": None,
            "target_operator": None,
            "type": None,
            "status": None,
            "channels": None,
            "whatsapp_template_instance": None,
            "created_at": None,
        }
    )


def _route_stop(**overrides):
    data = {
        "id": ROUTE_STOP_ID,
        "pickup_order_id": PICKUP_ORDER_ID,
        "stop_number": 2,
        "queue_position": 2,
        "clinic_name_snapshot": "Crubvet Talagante",
    }
    data.update(overrides)
    return data


class FakeTenantClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, path, *, json=None):
        self.calls.append({"path": path, "json": json})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeOrchestrator:
    def __init__(self, history_events=None):
        self.calls = []
        self.history_events = history_events or []

    def call(self, endpoint, **kwargs):
        self.calls.append({"endpoint": endpoint, **kwargs})
        if endpoint == "get_orchestration_events":
            return {"orchestration_events": self.history_events}
        if endpoint == "evolve_event":
            return {
                "status_code": 201,
                "uuid": "22222222-2222-4222-8222-222222222222",
                "extra_params": kwargs["extra_params"],
            }
        return {"status_code": 200}


def _http_404(message="No in-route or paused stop found for driver"):
    response = requests.Response()
    response.status_code = 404
    response.url = "https://gammavet.chask.co/api/gammavet/route-stops/fail-current"
    return requests.HTTPError(message, response=response)


def test_completar_parada_fallo_requires_nota(monkeypatch):
    fake_orchestrator = FakeOrchestrator()
    backend = FunctionBackend(_event({"route_stop_id": ROUTE_STOP_ID}))
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    with pytest.raises(ValueError, match="requiere nota"):
        backend.process_request()


def test_completar_parada_fallo_passes_exact_ids_and_failure_reason(monkeypatch):
    tenant_client = FakeTenantClient(
        [
            {
                "route_stop": _route_stop(status="fallo"),
                "next_route_stop": {"clinic_name_snapshot": "Clinica Siguiente"},
                "has_next_pending": True,
                "total_stops": 3,
            }
        ]
    )
    fake_orchestrator = FakeOrchestrator()
    backend = FunctionBackend(
        _event(
            {
                "route_stop_id": ROUTE_STOP_ID,
                "pickup_order_id": PICKUP_ORDER_ID,
                "driver_phone": "+56 9 1111 2222",
                "nota": "clinica cerrada",
            }
        )
    )
    monkeypatch.setattr(backend.context, "tenant_client", lambda: tenant_client)
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    result = backend.process_request()

    assert tenant_client.calls[0]["path"] == TENANT_FAIL_CURRENT_PATH
    payload = tenant_client.calls[0]["json"]
    assert payload["route_stop_id"] == ROUTE_STOP_ID
    assert payload["pickup_order_id"] == PICKUP_ORDER_ID
    assert payload["note"] == "clinica cerrada"
    assert payload["failure_reason"] == "clinica cerrada"
    assert "marcada como fallo" in result
    prompts = [c["prompt"] for c in fake_orchestrator.calls if c["endpoint"] == "evolve_event"]
    assert any("Registramos el problema" in prompt for prompt in prompts)


def test_completar_parada_fallo_blocks_mismatched_tenant_response(monkeypatch):
    tenant_client = FakeTenantClient(
        [{"route_stop": _route_stop(id=STALE_ROUTE_STOP_ID), "total_stops": 3}]
    )
    fake_orchestrator = FakeOrchestrator()
    backend = FunctionBackend(
        _event(
            {
                "route_stop_id": ROUTE_STOP_ID,
                "pickup_order_id": PICKUP_ORDER_ID,
                "nota": "no estaba disponible",
            }
        )
    )
    monkeypatch.setattr(backend.context, "tenant_client", lambda: tenant_client)
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    result = backend.process_request()

    assert "mismatch" in result
    dispatch_calls = [
        c for c in fake_orchestrator.calls
        if c["endpoint"] == "evolve_event" and c.get("event_type") == "dispatch_event"
    ]
    assert dispatch_calls[0]["extra_params"]["event_type"] == (
        "conductor_completion_route_stop_mismatch"
    )
    metadata = dispatch_calls[0]["extra_params"]["metadata"]
    assert metadata["outcome"] == "fallo"
    assert metadata["requested_route_stop_id"] == ROUTE_STOP_ID
    assert metadata["actual_route_stop_id"] == STALE_ROUTE_STOP_ID
    whatsapp_calls = [
        c for c in fake_orchestrator.calls
        if c["endpoint"] == "evolve_event"
        and c.get("event_type") == "response_to_whatsapp_message"
    ]
    assert whatsapp_calls == []


def test_completar_parada_fallo_no_active_stop_is_terminal(monkeypatch):
    tenant_client = FakeTenantClient([_http_404()])
    fake_orchestrator = FakeOrchestrator()
    backend = FunctionBackend(_event({"driver_phone": "+56 9 1111 2222", "nota": "cerrado"}))
    monkeypatch.setattr(backend.context, "tenant_client", lambda: tenant_client)
    monkeypatch.setattr("backend.conductor_common.orchestrator_api_manager", fake_orchestrator)

    result = backend.process_request()

    assert "Handoff terminal" in result
    dispatch_call = next(
        c for c in fake_orchestrator.calls
        if c["endpoint"] == "evolve_event" and c.get("event_type") == "dispatch_event"
    )
    assert dispatch_call["extra_params"]["event_type"] == (
        "conductor_complete_current_missing_terminal"
    )
