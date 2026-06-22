"""
Business logic for CompletarParadaFalloFn.

This dedicated lambda has no accion parameter. Selecting the "Reportar fallo"
node means the driver reported a real failed pickup attempt.
"""

import logging
from typing import Any

import requests
from chask_foundation.backend.models import OrchestrationEvent

from backend.conductor_common import (
    ESTADO_FALLO,
    TENANT_FAIL_CURRENT_PATH,
    TENANT_FAIL_CURRENT_ROUTE,
    ConductorContext,
    ConductorRuntime,
    tenant_data_public_test_mode,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RUNTIME = ConductorRuntime(
    actor_lambda="gammavet_completar_parada_fallo",
    function_uuid_default="00000000-0000-4000-8000-000000000002",
)


class FunctionBackend:
    """Register a genuine failed pickup for the requested Gammavet route stop."""

    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        self.context = ConductorContext(orchestration_event, RUNTIME)
        logger.info(
            "CompletarParadaFalloFn initialized for org: %s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        args = self.context.tool_args()
        nota = str(
            args.get("nota")
            or args.get("failure_reason")
            or args.get("motivo")
            or args.get("reason")
            or ""
        ).strip()
        if not nota:
            raise ValueError("CompletarParadaFalloFn requiere nota o failure_reason")

        resolved = self.context.resolve_route_stop_ids()
        payload = self.context.build_driver_action_payload()
        if resolved.route_stop_id:
            payload["route_stop_id"] = resolved.route_stop_id
        if resolved.pickup_order_id:
            payload["pickup_order_id"] = resolved.pickup_order_id
        payload["note"] = nota
        payload["failure_reason"] = nota

        logger.info(
            "CompletarParadaFalloFn fail-current driver_id=%s driver_phone=%s route_stop_id=%s pickup_order_id=%s event_id=%s",
            payload.get("driver_id"),
            payload.get("driver_phone"),
            payload.get("route_stop_id"),
            payload.get("pickup_order_id"),
            payload["orchestration_event_uuid"],
        )

        try:
            with tenant_data_public_test_mode():
                result = self.context.tenant_client().post(
                    TENANT_FAIL_CURRENT_PATH,
                    json=payload,
                )
        except requests.HTTPError as exc:
            if self.context.is_no_active_stop_http_404(exc):
                return self.context.complete_current_missing_terminal(exc)
            raise

        if not isinstance(result, dict):
            raise RuntimeError(
                f"Tenant API {TENANT_FAIL_CURRENT_ROUTE} devolvio una respuesta inesperada"
            )

        failed_stop = result.get("route_stop") or {}
        if self.context.completion_response_mismatched(
            failed_stop,
            requested_route_stop_id=resolved.route_stop_id,
            requested_pickup_order_id=resolved.pickup_order_id,
        ):
            self.context.emit_completion_mismatch(
                failed_stop,
                requested_route_stop_id=resolved.route_stop_id,
                requested_pickup_order_id=resolved.pickup_order_id,
                outcome=ESTADO_FALLO,
                endpoint_route=TENANT_FAIL_CURRENT_ROUTE,
            )
            return (
                "Bloqueada confirmacion de CompletarParadaFalloFn por mismatch de "
                "route_stop_id/pickup_order_id en respuesta Tenant API."
            )

        return self._notify_failure(result, failed_stop)

    def _notify_failure(self, result: dict[str, Any], failed_stop: dict[str, Any]) -> str:
        num_actual = failed_stop.get("stop_number") or failed_stop.get("queue_position") or "?"
        total = result.get("total_stops") or failed_stop.get("total_stops") or "?"
        clinic_name = (
            failed_stop.get("clinic_name_snapshot")
            or failed_stop.get("clinica")
            or "parada actual"
        )

        if result.get("has_next_pending") or result.get("next_route_stop"):
            self.context.enviar_mensaje_texto(
                "Registramos el problema con esta parada. "
                "Tienes una nueva ruta pendiente por completar; responde cuando quieras continuar."
            )
            next_stop = result.get("next_route_stop") or {}
            next_clinic = (
                next_stop.get("clinic_name_snapshot")
                or next_stop.get("clinica")
                or "siguiente parada"
            )
            return (
                f"Parada {num_actual}/{total} ({clinic_name}) marcada como fallo. "
                f"Siguiente parada pendiente: {next_clinic}. Mensaje enviado al conductor."
            )

        self.context.enviar_mensaje_texto(
            "Registramos el problema con esta parada. "
            "Por el momento no tienes rutas pendientes."
        )
        return (
            f"Parada {num_actual}/{total} ({clinic_name}) marcada como fallo. "
            "No hay mas paradas pendientes."
        )
