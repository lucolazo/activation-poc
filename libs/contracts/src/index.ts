import { randomUUID } from 'crypto';

/** Única fuente de verdad de los eventos (RNF-07). */
export const TOPICS = {
  REQUESTED: 'activation.requested',
  BILLING: 'billing.events',
  PROVISIONING: 'provisioning.events',
  ACTIVATION: 'activation.events',
} as const;
export const ALL_TOPICS: string[] = Object.values(TOPICS);
export const dlqOf = (topic: string) => `${topic}.dlq`;

export type FailureMode = 'none' | 'billing' | 'provisioning';

export type EventType =
  | 'ActivationRequested'
  | 'BillingAccountCreated' | 'BillingFailed' | 'BillingAccountCancelled'
  | 'ProvisioningCompleted' | 'ProvisioningFailed'
  | 'ActivationCompleted' | 'ActivationFailed';

export const TOPIC_BY_EVENT: Record<EventType, string> = {
  ActivationRequested: TOPICS.REQUESTED,
  BillingAccountCreated: TOPICS.BILLING,
  BillingFailed: TOPICS.BILLING,
  BillingAccountCancelled: TOPICS.BILLING,
  ProvisioningCompleted: TOPICS.PROVISIONING,
  ProvisioningFailed: TOPICS.PROVISIONING,
  ActivationCompleted: TOPICS.ACTIVATION,
  ActivationFailed: TOPICS.ACTIVATION,
};

/** Sobre común a todos los eventos. */
export interface EventEnvelope<P = Record<string, any>> {
  eventId: string;
  eventType: EventType;
  version: number;
  occurredAt: string;
  correlationId: string; // = activationId (RNF-06)
  customerId: string;    // = clave de partición (RNF-02)
  source: string;
  payload: P;
}

export function makeEvent<P = Record<string, any>>(
  eventType: EventType, source: string, correlationId: string,
  customerId: string, payload: P, version = 1,
): EventEnvelope<P> {
  return { eventId: randomUUID(), eventType, version, occurredAt: new Date().toISOString(),
           correlationId, customerId, source, payload };
}

export function isEnvelope(x: any): x is EventEnvelope {
  return !!x && typeof x.eventId === 'string' && x.eventType in TOPIC_BY_EVENT &&
    typeof x.version === 'number' && typeof x.correlationId === 'string' &&
    typeof x.customerId === 'string' && typeof x.payload === 'object';
}
