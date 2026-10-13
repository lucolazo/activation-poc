import 'reflect-metadata';
import { Injectable, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ClientSession } from 'mongodb';
import { EventEnvelope, EventType, TOPICS, makeEvent } from '@poc/contracts';
import { Mongo, addToOutbox, connectMongo, consume, createKafka, createProducer,
         processOnce, sleep, startOutboxRelay } from '@poc/kafka-toolkit';

@Injectable()
class Worker implements OnModuleInit {
  private m: Mongo;
  private get accounts() { return this.m.db.collection<any>('billing_accounts'); }

  async onModuleInit() {
    const kafka = createKafka('billing');
    this.m = await connectMongo('billing');
    await this.accounts.createIndex({ activationId: 1 }, { unique: true });
    startOutboxRelay(this.m, await createProducer(kafka));
    await consume({ kafka, groupId: process.env.KAFKA_GROUP_ID ?? 'billing-svc',
      topics: [TOPICS.REQUESTED, TOPICS.ACTIVATION], handler: (e) => this.handle(e) });
    console.log('[billing] listo');
  }

  private emit(s: ClientSession, src: EventEnvelope, type: EventType, payload = {}) {
    return addToOutbox(this.m, s, makeEvent(type, 'billing', src.correlationId, src.customerId, payload));
  }

  private async handle(e: EventEnvelope) {
    if (e.eventType === 'ActivationRequested') return this.onRequested(e);
    if (e.eventType === 'ActivationFailed') return this.onFailed(e);
  }

  private async onRequested(e: EventEnvelope) {
    await sleep(500 + Math.random() * 500);
    await processOnce(this.m, e, async (s) => {
      const id = e.correlationId;
      const prev = await this.accounts.findOne({ activationId: id }, { session: s });
      if (e.payload.simulateFailure === 'billing') { // error de negocio simulado: no va a la DLQ
        await this.emit(s, e, 'BillingFailed', { reason: 'Fallo simulado en facturación' });
        return;
      }
      if (!prev) {
        await this.accounts.insertOne({ activationId: id, customerId: e.customerId, planId: e.payload.planId,
          status: 'CREATED', createdAt: new Date() }, { session: s });
        await this.emit(s, e, 'BillingAccountCreated', { planId: e.payload.planId });
      } else if (prev.status === 'FAILED_SEEN') {
        // Caso borde: ya vimos ActivationFailed antes de crear -> se crea y se anula
        await this.accounts.updateOne({ activationId: id },
          { $set: { status: 'CANCELLED', cancelledAt: new Date() } }, { session: s });
        await this.emit(s, e, 'BillingAccountCreated', { planId: e.payload.planId });
        await this.emit(s, e, 'BillingAccountCancelled', { reason: 'Activación fallida' });
      }
    });
  }

  /** Compensación (RF-06). */
  private async onFailed(e: EventEnvelope) {
    await processOnce(this.m, e, async (s) => {
      const id = e.correlationId;
      const prev = await this.accounts.findOne({ activationId: id }, { session: s });
      if (prev?.status === 'CREATED') {
        await this.accounts.updateOne({ activationId: id },
          { $set: { status: 'CANCELLED', cancelledAt: new Date() } }, { session: s });
        await this.emit(s, e, 'BillingAccountCancelled', { reason: e.payload.reason });
      } else if (!prev) {
        await this.accounts.insertOne({ activationId: id, customerId: e.customerId, status: 'FAILED_SEEN',
          createdAt: new Date() }, { session: s });
      }
    });
  }
}

@Module({ providers: [Worker] })
class AppModule {}
NestFactory.createApplicationContext(AppModule).then((app) => app.enableShutdownHooks());
