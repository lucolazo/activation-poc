# Arquitectura de la POC

`activation-api` es el único punto de entrada; todo lo demás reacciona a eventos en Kafka.

```
Demo UI (React) --POST /activations--> activation-api --ActivationRequested--> [activation.requested]
     ^ WebSocket                           |  (outbox)                         |        |         |
     |                                     v                                   v        v         v
     +-------------------------------- MongoDB                            billing  provisioning  crm-analytics
                                                                              |        |
                    [billing.events] / [provisioning.events] <----------------+--------+
                              |  (activation-api agrega los resultados)
                              v
                    [activation.events]: ActivationCompleted | ActivationFailed
                         |          |             |
                  notification   billing      crm-analytics
                   (email)     (compensación)
```

| Topic | Productor | Consumidores (group) |
|---|---|---|
| activation.requested | activation-api | billing-svc, provisioning-svc, crm-analytics |
| billing.events | billing | activation-api, crm-analytics |
| provisioning.events | provisioning | activation-api, crm-analytics |
| activation.events | activation-api | notification-svc, billing-svc, crm-analytics |
| `<topic>.dlq` | el consumidor que falló | inspección manual en Kafka UI |

Todos con 3 particiones, retención de 7 días y `customerId` como clave.

## Decisiones técnicas implementadas
1. **Idempotencia**: `processOnce` (libs/kafka-toolkit) inserta el `eventId` en `processed_events` en la misma transacción Mongo que el efecto.
2. **Reintentos y DLQ**: 3 reintentos (1 s, 2 s, 4 s); luego se publica el mensaje original en `<topic>.dlq` con el error en headers. Los fallos simulados son eventos de negocio (`BillingFailed`/`ProvisioningFailed`), no van a la DLQ.
3. **Outbox**: activation-api, billing y provisioning escriben el evento en `outbox` dentro de la transacción; un relay lo publica cada 500 ms.
4. **Cliente Kafka**: kafkajs aislado en `libs/kafka-toolkit` (reemplazable). Los servicios son aplicaciones NestJS (módulos, DI, ciclo de vida, `@WebSocketGateway`).
5. **Commit de offset manual**, siempre después de procesar. Mongo en replica set de un nodo.
6. **Coreografía con agregador**; caso borde de `ActivationFailed` antes de crear la cuenta resuelto con el estado `FAILED_SEEN` en `billing_accounts`.

## Trazabilidad de requerimientos
| Req. | Dónde |
|---|---|
| RF-01, RF-08 | apps/demo-ui |
| RF-02, RF-03, RF-09 | activation-api (controller/service) |
| RF-04, RF-05, RF-06 | billing, provisioning, `onResult` en activation-api |
| RF-07 | notification-service |
| RF-10 | crm-analytics-service (`event_log`, `fromBeginning`) |
| RNF-01 | docker-compose.yml |
| RNF-02 | `publish()` usa `customerId` como key |
| RNF-03 | `processOnce` |
| RNF-04, RNF-05 | `consume()` (reintentos, DLQ, commit manual) |
| RNF-06 | `correlationId` = activationId en todos los eventos |
| RNF-07 | libs/contracts (campo `version`) |
