# Pick-Morph-Camunda — End-to-End Test Guide

Tests the full pick instruction workflow:
**AA receives instruction → AE accepts → bot picks item → order completes**

---

## Pre-requisites

- Docker Desktop running
- No stale postgres data from a previous run

---

## 0. Setup / Clean Start (run before every test session)

Use `setup.sh` — it detects whether the app is already running and takes the fastest path:

```bash
../setup.sh
```

### What it does

| Scenario | Actions |
|----------|---------|
| **spring-camunda is running** (hot) | ① Truncate `ae_order`, `transaction_status`, `outbox_event` <br>② Truncate Camunda runtime/history tables (`act_ru_*`, `act_hi_*`) <br>③ Delete Kafka topics <br>④ Rebuild & restart **only** `spring-camunda` (postgres/kafka untouched) |
| **Nothing is running** (cold) | `docker compose down -v` → `docker compose up --build -d` |

> The script waits for the health check to pass before exiting — safe to immediately run Step 1 once it completes.

### Manual setup (if needed)

```bash
# Full cold start
docker compose down -v
docker compose up --build -d
docker compose logs -f spring-camunda   # wait for "Started SpringCamundaApplication"

# Hot reset only (app already running)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c "
  TRUNCATE TABLE outbox_event, transaction_status, ae_order RESTART IDENTITY CASCADE;
  TRUNCATE TABLE
    act_hi_job_log, act_hi_ext_task_log, act_hi_op_log, act_hi_detail,
    act_hi_comment, act_hi_attachment, act_hi_identitylink, act_hi_varinst,
    act_hi_taskinst, act_hi_actinst, act_hi_incident, act_hi_batch, act_hi_procinst,
    act_ru_ext_task, act_ru_batch, act_ru_incident, act_ru_identitylink,
    act_ru_task, act_ru_variable, act_ru_event_subscr, act_ru_job, act_ru_execution
  CASCADE;
"
```

---

## Step 1 — Start workflow (AA receives pick instruction)

**What happens:** REST call starts the Camunda process, persists `ae_order` + outbox event,
publishes pick request to `gor.pick-list.requests`.

```bash
curl -s -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{
    "pickInstructionId": "PICK-001",
    "item": "Screwdriver",
    "tpid": "TPID-100",
    "qty": 2,
    "uom": "BOX",
    "pickLocation": "ZONE-A",
    "dropLocation": "PACK-01",
    "ppsId": 1,
    "binId": "BIN-01",
    "ppsPoint": "PPS-01",
    "seatName": "SEAT-01",
    "slotref": "SLOT-01",
    "userLoggedIn": "op1"
  }' | jq .
```

**Verify:**

```bash
# ae_order row created
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT id, order_id, status FROM ae_order;"

# outbox_event row PENDING → PUBLISHED (within ~5 s)
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT aggregate_id, topic, status FROM outbox_event ORDER BY created_at DESC LIMIT 5;"

# Kafka UI — check gor.pick-list.requests has a new message
# http://localhost:8080
```

---

## Step 2 — AE accepts pick request (response from AE)

**What happens:** AE publishes SUCCESS response to `gor.pick-list.response`.
`PickListResponseListener` correlates the Camunda message; workflow advances.

```bash
echo '{"externalServiceRequestId":"PICK-001","status":"SUCCESS","success":true}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Failure scenario (AE rejects):**

```bash
echo '{"externalServiceRequestId":"PICK-001","status":"FAILURE","errorCode":"VALIDATION_FAILED","message":"Item not found"}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Verify:**

```bash
docker compose logs spring-camunda | grep -i "PICK-001"
# Expect: "AE accepted" or "PickList response received" log lines
```

---

## Step 3 — Bot picks item (pick_transaction event)

**What happens:** AE publishes a `pick_transaction` event to `gor.pick-list.events`.
`PickListEventListener` saves a `transaction_status` row + outbox event, publishes to `gor.item_picked.events`.

```bash
echo '{
  "headers": {"event_type": "pick_transaction"},
  "value": {
    "payload": {
      "externalServiceRequestId": "PICK-001",
      "serviceRequests": [
        {
          "externalServiceRequestId": "PICK-001",
          "transactions": [
            {
              "transactionId": "TXN-001",
              "transactionStatus": "PROCESSED",
              "state": "complete",
              "type": "PICK",
              "containerAttributes": {
                "internal_order_id": 9001,
                "lpn_id": 42,
                "qty_to_be_picked": 2,
                "qty_picked": 2,
                "status": "complete",
                "bot_id": "BOT-07",
                "pps_id": "PPS-01"
              }
            }
          ]
        }
      ]
    }
  }
}' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.events
```

**Verify:**

```bash
# transaction_status row created
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status FROM transaction_status;"

# outbox_event for item_picked.events published
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status FROM outbox_event ORDER BY created_at DESC LIMIT 5;"

# Kafka UI — check gor.item_picked.events has a new message
# http://localhost:8080
```

**Expected `gor.item_picked.events` payload:**

```json
{
  "pps_id": "PPS-01",
  "seat_name": "SEAT-01",
  "order_id": "PICK-001",
  "slot_ref": "SLOT-01",
  "pps_bin_id": "BIN-01",
  "transaction_id": "TXN-001",
  "state": "complete",
  "pps_point": "PPS-01",
  "user_logged_in": "op1",
  "dangling_area": "bot",
  "picked_item_info_list": [
    {
      "tpid": "TPID-100",
      "item_uid": "Screwdriver",
      "uom": "BOX",
      "picked_qty": 2,
      "pick_instruction_ids": ["PICK-001"]
    }
  ]
}
```

> `dangling_area` = `"bot"` is set automatically for all `pick_transaction` events.

---

## Step 4 — Order complete (update event from AE)

**What happens:** AE publishes an `update` event with `state=complete`.
`PickListEventListener` correlates `TransactionUpdateMessage` → workflow reaches end state.

```bash
echo '{
  "headers": {"event_type": "update"},
  "value": {
    "payload": {
      "externalServiceRequestId": "PICK-001",
      "state": "complete",
      "attributes": {
        "sub_state": "complete"
      }
    }
  }
}' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.events
```

**Verify:**

```bash
# ae_order status should be COMPLETE
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT order_id, status FROM ae_order;"

# Camunda process instance should be ended (no active rows)
docker compose logs spring-camunda | grep -i "workflow complete\|process ended\|PICK-001"
```

---

## Step 5 — Cancellation scenario (optional)

Send `state=cancellation_locked` to trigger the cancel path:

```bash
echo '{
  "headers": {"event_type": "update"},
  "value": {
    "payload": {
      "externalServiceRequestId": "PICK-001",
      "state": "cancellation_locked",
      "attributes": {"sub_state": "cancelled"}
    }
  }
}' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.events
```

---

## Useful Queries

```bash
# All tables
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "\dt"

# Full ae_order row
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT * FROM ae_order WHERE order_id='PICK-001';"

# All outbox events
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT id, aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at;"

# All transaction statuses
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT * FROM transaction_status;"

# Active Camunda process instances
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT proc_inst_id_, business_key_, start_time_ FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 10;"

# App health
curl -s http://localhost:9191/actuator/health | jq .
```

---

## Useful UIs

| Tool | URL |
|------|-----|
| Kafka UI | http://localhost:8080 |
| Camunda Cockpit | http://localhost:9191/camunda/app/cockpit (admin / admin) |
| App health | http://localhost:9191/actuator/health |

---

## Kafka Topics Reference

| Topic | Direction | Trigger |
|-------|-----------|---------|
| `gor.pick-list.requests` | Outbound (AA → AE) | Step 1 — workflow start |
| `gor.pick-list.response` | Inbound (AE → AA) | Step 2 — AE accepts/rejects |
| `gor.pick-list.events` | Inbound (AE → AA) | Steps 3 & 4 — pick_transaction, update |
| `gor.item_picked.events` | Outbound (AA → Butler Core) | Step 3 — per pick_transaction |
| `gor.pick-instruction.events` | Inbound (optional) | Alternative trigger via Kafka instead of REST |
