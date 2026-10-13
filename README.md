# POC Activación de Servicios con Kafka
Kafka (KRaft) + NestJS + React + MongoDB. Ver `docs/architecture.md` y `docs/demo-script.md`.

```bash
docker compose up --build        # RNF-01
# UI http://localhost:5173 · API http://localhost:3000 · Kafka UI http://localhost:8080 · Mailhog http://localhost:8025
./scripts/smoke-test.sh
```
Estructura: `apps/` (5 servicios NestJS + loyalty + demo-ui), `libs/contracts`, `libs/kafka-toolkit`, `docker/`, `docs/`.
