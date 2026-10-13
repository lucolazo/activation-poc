import 'reflect-metadata';
import { Injectable, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { EventEnvelope, TOPICS, makeEvent } from '@poc/contracts';
import { Mongo, addToOutbox, connectMongo, consume, createKafka, createProducer,
         processOnce, sleep, startOutboxRelay } from '@poc/kafka-toolkit';

@Injectable()
class Worker implements OnModuleInit {
  private m: Mongo;

  async onModuleInit() {
    const kafka = createKafka('provisioning');
    this.m = await connectMongo('provisioning');
    await this.m.db.collection('provisioning_orders').createIndex({ activationId: 1 }, { unique: true });
    startOutboxRelay(this.m, await createProducer(kafka));
    await consume({ kafka, groupId: process.env.KAFKA_GROUP_ID ?? 'provisioning-svc',
      topics: [TOPICS.REQUESTED], handler: (e) => this.handle(e) });
    console.log('[provisioning] listo');
  }

  private async handle(e: EventEnvelope) {
    if (e.eventType !== 'ActivationRequested') return;
    await sleep(1000 + Math.random() * 2000); // retardo simulado de 1 a 3 s
    await processOnce(this.m, e, async (s) => {
      const failed = e.payload.simulateFailure === 'provisioning';
      await this.m.db.collection<any>('provisioning_orders').insertOne({
        activationId: e.correlationId, customerId: e.customerId, planId: e.payload.planId,
        status: failed ? 'FAILED' : 'COMPLETED', createdAt: new Date() }, { session: s });
      const ev = failed
        ? makeEvent('ProvisioningFailed', 'provisioning', e.correlationId, e.customerId,
            { reason: 'Fallo simulado en aprovisionamiento' })
        : makeEvent('ProvisioningCompleted', 'provisioning', e.correlationId, e.customerId,
            { planId: e.payload.planId });
      await addToOutbox(this.m, s, ev);
    });
  }
}

@Module({ providers: [Worker] })
class AppModule {}
NestFactory.createApplicationContext(AppModule).then((app) => app.enableShutdownHooks());
