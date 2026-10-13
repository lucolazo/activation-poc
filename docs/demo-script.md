# Guión de demo (≈15 min)
Pantalla dividida: Demo UI (http://localhost:5173) a la izquierda, Kafka UI (http://localhost:8080) a la derecha. Emails en Mailhog: http://localhost:8025.

| # | Escenario | Qué se hace | Qué se ve | Concepto |
|---|---|---|---|---|
| 1 | Camino feliz | Contratar un plan sin fallo | Timeline con 4 eventos en verde; email en Mailhog | Fan-out |
| 2 | Fallo y compensación | Contratar con fallo en provisioning | ProvisioningFailed, ActivationFailed, BillingAccountCancelled; email de error | Saga sin llamadas directas |
| 3 | Servicio caído | Detener notification, crear 3 activaciones, volver a levantarlo | Lag de 3 en Kafka UI; al volver llegan los 3 emails | Offsets |
| 4 | Escalar | Levantar una segunda instancia de billing | Kafka UI reparte las 3 particiones entre 2 consumidores | Consumer group |
| 5 | Nuevo consumidor | Levantar loyalty-service | Procesa todo el historial sin tocar a los demás | Replay del log |

```bash
# Escenario 3
docker compose stop notification-service
docker compose start notification-service
# Escenario 4
docker compose up -d --scale billing-service=2
# Escenario 5
docker compose --profile loyalty up -d loyalty-service
```

## Criterios de aceptación
- `docker compose up --build` levanta todo sin pasos manuales.
- Reenviar un evento ya procesado no duplica cuentas ni emails: en Kafka UI, *Produce Message* con el mismo JSON (mismo `eventId`) en `activation.events`; Mailhog sigue mostrando un solo email.
- Mensaje inválido a la DLQ: `./scripts/send-invalid-message.sh` y revisar `activation.requested.dlq` (tarda ~7 s por los reintentos solo si el error es técnico; los inválidos van directo).
- Trazabilidad: `GET /activations/:id` o la colección `event_log` filtrada por `correlationId`.
- Prueba rápida de punta a punta: `./scripts/smoke-test.sh`.
