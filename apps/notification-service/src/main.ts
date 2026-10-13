import 'reflect-metadata';
import { Injectable, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import * as nodemailer from 'nodemailer';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { Mongo, connectMongo, consume, createKafka, processOnce } from '@poc/kafka-toolkit';

@Injectable()
class Worker implements OnModuleInit {
  private m: Mongo;
  private mail = nodemailer.createTransport({ host: process.env.SMTP_HOST ?? 'mailhog', port: 1025 });

  async onModuleInit() {
    const kafka = createKafka('notification');
    this.m = await connectMongo('notification');
    await this.m.db.collection('notifications').createIndex({ activationId: 1, eventType: 1 }, { unique: true });
    await consume({ kafka, groupId: process.env.KAFKA_GROUP_ID ?? 'notification-svc',
      topics: [TOPICS.ACTIVATION], handler: (e) => this.handle(e) });
    console.log('[notification] listo');
  }

  private async handle(e: EventEnvelope) {
    const ok = e.eventType === 'ActivationCompleted';
    if (!ok && e.eventType !== 'ActivationFailed') return;
    await processOnce(this.m, e, async (s) => {
      const to = `${e.customerId}@example.com`;
      // Se registra primero: si el envío falla, la transacción se revierte y se reintenta.
      await this.m.db.collection<any>('notifications').insertOne(
        { activationId: e.correlationId, eventType: e.eventType, to, sentAt: new Date() }, { session: s });
      await this.mail.sendMail({
        from: 'activaciones@poc.local', to,
        subject: ok ? `¡Bienvenido! Tu plan ${e.payload.planId} está activo`
                    : 'No pudimos activar tu plan',
        text: ok ? `Activación ${e.correlationId} completada.`
                 : `Activación ${e.correlationId} fallida: ${e.payload.reason}.`,
      });
    });
  }
}

@Module({ providers: [Worker] })
class AppModule {}
NestFactory.createApplicationContext(AppModule).then((app) => app.enableShutdownHooks());
