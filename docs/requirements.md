# Webhook Delivery Platform

## Goal

Build a reliable webhook delivery platform that allows external systems
to publish events and allows clients to subscribe to specific event types.

## Functional Requirements

### Webhook Subscriptions

- A client can create a webhook subscription.
- A client can subscribe to one or more event types.
- A client can delete a subscription.

### Events

- External systems can publish events.
- Each event has a unique event ID.
- An event contains an event type and payload.
- The system delivers an event to all matching subscriptions.

### Delivery

- Webhook delivery is asynchronous.
- Each delivery attempt is recorded.
- Failed deliveries are retried.
- Retries use exponential backoff.
- Events that permanently fail are moved to a DLQ.

## Non-Functional Requirements

- PostgreSQL is the source of truth.
- The system supports multiple delivery workers.
- The system should tolerate individual worker failures.
- The system should avoid duplicate processing where possible.
- The system should be horizontally scalable.
- A slow or unavailable webhook consumer should not block other consumers.