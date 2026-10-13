import 'reflect-metadata';
import { Injectable, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ALL_TOPICS, EventEnvelope } from '@poc/contracts';
import { Mongo, connectMongo, consume, createKafka, processOnce } from '@poc/kafka-toolkit';

/** Solo escucha: registra todos los eventos y puede reprocesar desde el inicio (RF-10). */
@Injectable()
class Worker implements OnModuleInit {
  private m: Mongo;

  async onModuleInit() {
    const kafka = createKafka('crm-analytics');
    this.m = await connectMongo('crm_analytics');
    const log = this.m.db.collection('event_log');
    await log.createIndex({ eventId: 1 }, { unique: true });
    await log.createIndex({ correlationId: 1 });
    await consume({ kafka, groupId: process.env.KAFKA_GROUP_ID ?? 'crm-analytics',
      topics: ALL_TOPICS, fromBeginning: true, handler: (e) => this.handle(e) });
    console.log('[crm-analytics] listo');
  }

  private async handle(e: EventEnvelope) {
    await processOnce(this.m, e, async (s) => {
      await this.m.db.collection<any>('event_log').insertOne({ ...e, receivedAt: new Date() }, { session: s });
    });
  }
}

@Module({ providers: [Worker] })
class AppModule {}
NestFactory.createApplicationContext(AppModule).then((app) => app.enableShutdownHooks());
