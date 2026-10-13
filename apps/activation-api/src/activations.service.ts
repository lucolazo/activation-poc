import { BadRequestException, Injectable, OnModuleInit } from "@nestjs/common";
import { randomUUID } from "crypto";
import { EventEnvelope, FailureMode, TOPICS, makeEvent } from "@poc/contracts";
import {
  Mongo,
  addToOutbox,
  connectMongo,
  consume,
  createKafka,
  createProducer,
  processOnce,
  startOutboxRelay,
} from "@poc/kafka-toolkit";
import { EventsGateway } from "./events.gateway";

const MODES: FailureMode[] = ["none", "billing", "provisioning"];

@Injectable()
export class ActivationsService implements OnModuleInit {
  private m: Mongo;
  constructor(private readonly gateway: EventsGateway) {}

  private get col() {
    return this.m.db.collection<any>("activations");
  }

  async onModuleInit() {
    const kafka = createKafka("activation-api");
    this.m = await connectMongo("activation_api");
    await this.col.createIndex({ customerId: 1 });
    startOutboxRelay(this.m, await createProducer(kafka));
    await consume({
      kafka,
      groupId: process.env.KAFKA_GROUP_ID ?? "activation-api",
      topics: [TOPICS.BILLING, TOPICS.PROVISIONING],
      handler: (e) => this.onResult(e),
    });
  }

  get(id: string) {
    return this.col.findOne({ _id: id });
  }

  async create(dto: {
    customerId?: string;
    planId?: string;
    simulateFailure?: FailureMode;
  }) {
    const { customerId, planId } = dto ?? {};
    const simulateFailure = dto?.simulateFailure ?? "none";
    if (!customerId || !planId)
      throw new BadRequestException("customerId y planId son obligatorios");
    if (!MODES.includes(simulateFailure))
      throw new BadRequestException("simulateFailure inválido");

    const id = `act-${randomUUID().slice(0, 8)}`; // = correlationId
    const now = new Date().toISOString();
    const event = makeEvent(
      "ActivationRequested",
      "activation-api",
      id,
      customerId,
      { planId, channel: "web", simulateFailure },
    );
    const doc = {
      _id: id,
      customerId,
      planId,
      status: "PENDING",
      simulateFailure,
      steps: {},
      history: [{ eventType: "ActivationRequested", at: now }],
      createdAt: now,
      updatedAt: now,
    };

    // Outbox: activación y evento en la misma transacción (RF-03)
    const session = this.m.client.startSession();
    try {
      await session.withTransaction(async () => {
        await this.col.insertOne(doc, { session });
        await addToOutbox(this.m, session, event);
      });
    } finally {
      await session.endSession();
    }

    this.gateway.broadcast(doc);
    return { activationId: id, status: "PENDING" };
  }

  /** Agregador de la saga (RF-04..RF-06). */
  private async onResult(e: EventEnvelope) {
    const id = e.correlationId;
    const applied = await processOnce(this.m, e, async (session) => {
      const a = await this.col.findOne({ _id: id }, { session });
      if (!a) return; // evento de una activación desconocida
      const at = new Date().toISOString();
      const steps: any = { ...a.steps };
      const history: any[] = [{ eventType: e.eventType, at }];

      switch (e.eventType) {
        case "BillingAccountCreated":
          steps.billing = { status: "OK", at };
          break;
        case "BillingFailed":
          steps.billing = { status: "FAILED", at, reason: e.payload.reason };
          break;
        case "ProvisioningCompleted":
          steps.provisioning = { status: "OK", at };
          break;
        case "ProvisioningFailed":
          steps.provisioning = {
            status: "FAILED",
            at,
            reason: e.payload.reason,
          };
          break;
        default:
          break; // BillingAccountCancelled: solo historial
      }

      let status: string = a.status;
      if (status === "PENDING" || status === "IN_PROGRESS") {
        const failed = (["billing", "provisioning"] as const).find(
          (s) => steps[s]?.status === "FAILED",
        );
        if (failed) {
          status = "FAILED";
          history.push({ eventType: "ActivationFailed", at });
          await addToOutbox(
            this.m,
            session,
            makeEvent("ActivationFailed", "activation-api", id, a.customerId, {
              reason: steps[failed].reason,
              failedStep: failed,
            }),
          );
        } else if (
          steps.billing?.status === "OK" &&
          steps.provisioning?.status === "OK"
        ) {
          status = "ACTIVE";
          history.push({ eventType: "ActivationCompleted", at });
          await addToOutbox(
            this.m,
            session,
            makeEvent(
              "ActivationCompleted",
              "activation-api",
              id,
              a.customerId,
              { planId: a.planId },
            ),
          );
        } else status = "IN_PROGRESS";
      } // si ya es ACTIVE/FAILED: solo se registra en el historial

      await this.col.updateOne(
        { _id: id },
        {
          $set: { steps, status, updatedAt: at },
          $push: { history: { $each: history } },
        } as any,
        { session },
      );
    });
    if (applied) {
      const doc = await this.get(id);
      if (doc) this.gateway.broadcast(doc);
    }
  }
}
