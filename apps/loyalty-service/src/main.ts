import 'reflect-metadata';
import { Injectable, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ALL_TOPICS, EventEnvelope } from '@poc/contracts';
import { Mongo, connectMongo, consume, createKafka, processOnce } from '@poc/kafka-toolkit';

/** Solo escucha: nuevo consumidor (escenario 5): lee todo el historial sin tocar a los demás. */
@Injectable()
class Worker implements OnModuleInit {
  private m: Mongo;

  async onModuleInit() {
    const kafka = createKafka('loyalty');
    this.m = await connectMongo('loyalty');
    const log = this.m.db.collection('loyalty_log');
    await log.createIndex({ eventId: 1 }, { unique: true });
    await log.createIndex({ correlationId: 1 });
    await consume({ kafka, groupId: process.env.KAFKA_GROUP_ID ?? 'loyalty',
      topics: ALL_TOPICS, fromBeginning: true, handler: (e) => this.handle(e) });
    console.log('[loyalty] listo');
  }

  private async handle(e: EventEnvelope) {
    await processOnce(this.m, e, async (s) => {
      await this.m.db.collection<any>('loyalty_log').insertOne({ ...e, receivedAt: new Date() }, { session: s });
    });
  }
}

@Module({ providers: [Worker] })
class AppModule {}
NestFactory.createApplicationContext(AppModule).then((app) => app.enableShutdownHooks());
