/**
 * Toolkit: aísla el cliente Kafka (kafkajs) y concentra idempotencia,
 * reintentos, DLQ y outbox, para no repetirlos en cada servicio.
 */
import { Kafka, Producer, logLevel } from 'kafkajs';
import { ClientSession, Db, MongoClient } from 'mongodb';
import { EventEnvelope, TOPIC_BY_EVENT, dlqOf, isEnvelope } from '@poc/contracts';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const SEVEN_DAYS_S = 7 * 24 * 3600;

export function createKafka(clientId = process.env.KAFKA_CLIENT_ID ?? 'poc'): Kafka {
  return new Kafka({
    clientId, logLevel: logLevel.WARN,
    brokers: (process.env.KAFKA_BROKERS ?? 'kafka:9092').split(','),
    retry: { retries: 15, initialRetryTime: 500 },
  });
}

export async function createProducer(kafka: Kafka): Promise<Producer> {
  const p = kafka.producer({ allowAutoTopicCreation: false });
  await p.connect();
  return p;
}

export async function publish(producer: Producer, e: EventEnvelope) {
  await producer.send({
    topic: TOPIC_BY_EVENT[e.eventType],
    messages: [{ key: e.customerId, value: JSON.stringify(e),
                 headers: { eventType: e.eventType, correlationId: e.correlationId } }],
  });
}

// ---------- Mongo ----------
export interface Mongo { client: MongoClient; db: Db }

export async function connectMongo(dbName: string): Promise<Mongo> {
  const client = new MongoClient(process.env.MONGO_URI ?? 'mongodb://mongo:27017/?replicaSet=rs0');
  await client.connect();
  const db = client.db(dbName);
  await db.collection('processed_events').createIndex({ eventId: 1 }, { unique: true });
  await db.collection('processed_events').createIndex({ at: 1 }, { expireAfterSeconds: SEVEN_DAYS_S });
  await db.collection('outbox').createIndex({ status: 1, createdAt: 1 });
  return { client, db };
}

// ---------- Idempotencia (RNF-03) ----------
/** Registra el eventId y ejecuta el efecto en la MISMA transacción. Devuelve false si era duplicado. */
export async function processOnce(
  m: Mongo, e: EventEnvelope, effect: (s: ClientSession) => Promise<void>,
): Promise<boolean> {
  const session = m.client.startSession();
  let applied = false;
  try {
    await session.withTransaction(async () => {
      applied = false;
      const seen = m.db.collection('processed_events');
      if (await seen.findOne({ eventId: e.eventId }, { session })) return; // duplicado: se descarta
      await seen.insertOne({ eventId: e.eventId, at: new Date() }, { session });
      await effect(session);
      applied = true;
    });
    return applied;
  } finally {
    await session.endSession();
  }
}

// ---------- Outbox transaccional ----------
export function addToOutbox(m: Mongo, s: ClientSession, e: EventEnvelope) {
  return m.db.collection<any>('outbox').insertOne(
    { _id: e.eventId, event: e, status: 'PENDING', createdAt: new Date() }, { session: s });
}

/** Lee el outbox cada 500 ms, publica los pendientes y los marca como enviados. */
export function startOutboxRelay(m: Mongo, producer: Producer, intervalMs = 500) {
  let busy = false;
  setInterval(async () => {
    if (busy) return;
    busy = true;
    try {
      const col = m.db.collection<any>('outbox');
      const pending = await col.find({ status: 'PENDING' }).sort({ createdAt: 1 }).limit(50).toArray();
      for (const d of pending) {
        await publish(producer, d.event);
        await col.updateOne({ _id: d._id }, { $set: { status: 'SENT', sentAt: new Date() } });
      }
    } catch (err) {
      console.error('[outbox]', (err as Error).message);
    } finally { busy = false; }
  }, intervalMs);
}

// ---------- Consumo con reintentos + DLQ + commit manual (RNF-04, RNF-05) ----------
export interface ConsumeOptions {
  kafka: Kafka; groupId: string; topics: string[];
  handler: (e: EventEnvelope) => Promise<void>;
  fromBeginning?: boolean; maxRetries?: number;
}

export async function consume(o: ConsumeOptions) {
  const retries = o.maxRetries ?? 3;
  const consumer = o.kafka.consumer({ groupId: o.groupId, sessionTimeout: 10000, heartbeatInterval: 3000 });
  const dlq = await createProducer(o.kafka);
  await consumer.connect();
  await consumer.subscribe({ topics: o.topics, fromBeginning: o.fromBeginning ?? true });

  await consumer.run({
    autoCommit: false, // el offset se confirma después de procesar, nunca antes
    eachMessage: async ({ topic, partition, message }) => {
      let reason: string | undefined;
      let event: EventEnvelope | undefined;
      try {
        const parsed = JSON.parse(message.value?.toString() ?? '');
        if (!isEnvelope(parsed)) throw new Error('sobre de evento inválido');
        event = parsed;
      } catch (err) { reason = `mensaje inválido: ${(err as Error).message}`; }

      if (event) {
        for (let attempt = 0; attempt <= retries; attempt++) {
          try { await o.handler(event); reason = undefined; break; }
          catch (err) {
            reason = (err as Error).message;
            console.warn(`[${o.groupId}] intento ${attempt + 1} falló: ${reason}`);
            if (attempt < retries) await sleep(1000 * 2 ** attempt); // 1s, 2s, 4s
          }
        }
      }
      if (reason) {
        await dlq.send({ topic: dlqOf(topic), messages: [{
          key: message.key, value: message.value,
          headers: { 'x-error': reason, 'x-origin-topic': topic, 'x-group': o.groupId,
                     'x-partition': String(partition), 'x-offset': message.offset } }] });
        console.error(`[${o.groupId}] mensaje enviado a ${dlqOf(topic)}: ${reason}`);
      }
      await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
    },
  });
  return consumer;
}
