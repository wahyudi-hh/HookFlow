             ┌──────────────┐
             │ Webhook API  │
             └──────┬───────┘
                    │
                    ▼
                PostgreSQL
                    │
                    ▼
                  Kafka
                    │
                    ▼
             Delivery Worker
                    │
                    ▼
             Customer Webhook