# Pick-Morph-Camunda — Kellanova End-to-End Test Guide

Tests the full pick instruction workflow using **production-like Kellanova payloads** from the
[Pick AE Orchestrator Confluence page](https://greyorange-work.atlassian.net/wiki/spaces/BS/pages/134348801/Pick+AE+Orchestrator).

---

## Test Data Reference

| Field | Value | Source |
|-------|-------|--------|
| `pickInstructionId` | `Kellanova_order999977132` | `externalServiceRequestId` in AE order contract |
| `transactionId` | `1772445745488` | `transactions[].transactionId` in pick_transaction event |
| `internal_order_id` | `24234242` | `containerAttributes.internal_order_id` |
| `product_sku` | `10840243112927` | `productAttributes.product_sku` (expectations) |
| `product_uid` | `867` | `actuals.containers.products[].uid` |
| `qty` | `10` | `productQuantity` |
| `bot_id / pps_id` | `RIL-L-8301` | `containerAttributes.bot_id / pps_id` |
| `user_name` | `grey7` | `containerAttributes.user_name` |

---

## 0. Setup / Clean Start

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

### Manual cleanup (if needed)

```bash
# Truncate app + Camunda tables only
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

# Delete Kafka topics
for t in gor.pick-list.requests gor.pick-list.response gor.pick-list.events gor.item_picked.events; do
  docker exec pick-morph-camunda-kafka-1 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t
done

# Rebuild & restart spring-camunda only
docker compose build spring-camunda && \
docker compose up -d --no-deps --force-recreate spring-camunda
```

---

## Step 1 — Start Workflow (AA receives pick instruction)

**What happens:** REST call starts the Camunda process, persists `ae_order` + outbox event,
publishes pick request to `gor.pick-list.requests`.

```bash
curl -s -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{
    "pickInstructionId": "Kellanova_order999977132",
    "item": "10840243112927",
    "tpid": "2102",
    "qty": 10,
    "uom": "Item",
    "pickLocation": "NB-56-A",
    "dropLocation": "DOCK_DOOR2",
    "ppsId": 8301,
    "binId": "pps_bin_001",
    "ppsPoint": "PPS-POINT-1",
    "seatName": "seat_1",
    "slotref": "R002.1.3.2",
    "userLoggedIn": "grey7",
    "scannableBarcodes": ["10840243112927"]
  }' | jq .
```

**Verify:**

```bash
# ae_order row created
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT order_id, status FROM ae_order;"

# outbox_event PENDING → PUBLISHED (within ~5 s)
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status FROM outbox_event ORDER BY created_at DESC LIMIT 5;"

# Kafka UI: check gor.pick-list.requests → http://localhost:8080
```

---

## Step 2 — AE Accepts Pick Request

**What happens:** AE publishes SUCCESS response to `gor.pick-list.response`.
`PickListResponseListener` correlates the Camunda message; workflow advances.

```bash
echo '{
  "externalServiceRequestId": "Kellanova_order999977132",
  "serviceRequestId": 123456,
  "executionId": "exec_abc123def456",
  "status": "SUCCESS",
  "message": "Order created successfully in SRMS",
  "serviceRequests": [
    {
      "externalServiceRequestId": "Kellanova_order999977132_1",
      "serviceRequestId": 123457
    }
  ],
  "timestamp": "2026-02-26T10:00:00.000Z"
}' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Failure scenario (AE rejects):**

```bash
echo '{
  "externalServiceRequestId": "Kellanova_order999977132",
  "status": "FAILURE",
  "errorCode": "VALIDATION_FAILED",
  "message": "ServiceRequest validation failed",
  "errors": [
    {
      "field": "serviceRequests",
      "code": "MISSING_ORDER_LINES",
      "message": "ServiceRequests array is required and cannot be empty"
    }
  ],
  "timestamp": "2026-02-26T10:00:00.000Z"
}' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.response
```

**Verify:**

```bash
docker compose logs spring-camunda | grep -i "Kellanova_order999977132"
# Expect: "AE accepted" or "PickList response received" log lines
```

---

## Step 3 — Bot Picks Item (pick_transaction event)

**What happens:** AE publishes a `pick_transaction` event to `gor.pick-list.events`.
`PickListEventListener` correlates `ItemPickingEventMessage`; `SendItemPickedEventDelegate`
saves `transaction_status` + outbox event atomically, then relay publishes to `gor.item_picked.events`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.events
{
  "headers": {
    "event_type": "pick_transaction"
  },
  "value": {
    "name": "order_information",
    "payload": {
      "id": 12761,
      "externalServiceRequestId": "Kellanova_order999977132",
      "serviceRequests": [
        {
          "id": 12762,
          "externalServiceRequestId": "Kellanova_order999977132_1",
          "serviceRequests": [],
          "type": "PICK_LINE",
          "actuals": {
            "containers": [
              {
                "id": 2389529,
                "state": "complete",
                "type": "VIRTUAL",
                "barcode": null,
                "containers": [],
                "actions": [],
                "createdOn": "2026-03-02T10:02:27.987Z",
                "updatedOn": "2026-03-02T10:02:27.989Z",
                "transactionId": "1772445745488",
                "products": [
                  {
                    "id": 2389907,
                    "uid": "867",
                    "possibleUids": null,
                    "uidType": null,
                    "createdOn": "2026-03-02T10:02:27.988Z",
                    "updatedOn": "2026-03-02T10:02:27.988Z",
                    "productQuantity": 10,
                    "productAttributes": {
                      "tote_id": "C2554495756DOCK_DOOR204",
                      "package_count": 10,
                      "pdfa_values": {"product_sku": "10840243142832"},
                      "package_name": "undefined",
                      "serialized_content": [],
                      "tote_ids": ["C2554495756DOCK_DOOR204"]
                    }
                  }
                ],
                "containerAttributes": {
                  "internal_order_id": 24234242,
                  "lpn_id": 24234242,
                  "qty_to_be_picked": 10,
                  "qty_picked": 10,
                  "status": "complete",
                  "tote_id": "C2554495756DOCK_DOOR204",
                  "pps_bin_id": "undefined",
                  "pps_seat_name": "undefined",
                  "trueCopyIdOf": 1816456,
                  "user_name": "grey7",
                  "rollcage_id": "",
                  "pps_id": "RIL-L-8301",
                  "destination_location": "",
                  "location": "19-038-A",
                  "bot_id": "RIL-L-8301",
                  "exceptions": []
                },
                "carrier_type": null,
                "carrier_sub_type": null
              }
            ]
          },
          "transactions": [
            {
              "id": 2389529,
              "state": "complete",
              "type": "VIRTUAL",
              "barcode": null,
              "containers": [],
              "actions": [],
              "createdOn": "2026-03-02T10:02:27.987Z",
              "updatedOn": "2026-03-02T10:02:27.989Z",
              "transactionId": "1772445745488",
              "products": [
                {
                  "id": 2389907,
                  "uid": "867",
                  "possibleUids": null,
                  "uidType": null,
                  "createdOn": "2026-03-02T10:02:27.988Z",
                  "updatedOn": "2026-03-02T10:02:27.988Z",
                  "productQuantity": 10,
                  "productAttributes": {
                    "tote_id": "C2554495756DOCK_DOOR204",
                    "package_count": 10,
                    "pdfa_values": {"product_sku": "10840243142832"},
                    "package_name": "undefined",
                    "serialized_content": [],
                    "tote_ids": ["C2554495756DOCK_DOOR204"]
                  }
                }
              ],
              "containerAttributes": {
                "internal_order_id": 24234242,
                "lpn_id": 24234242,
                "qty_to_be_picked": 10,
                "qty_picked": 10,
                "status": "complete",
                "tote_id": "C2554495756DOCK_DOOR204",
                "pps_bin_id": "undefined",
                "pps_seat_name": "undefined",
                "trueCopyIdOf": 1816456,
                "user_name": "grey7",
                "rollcage_id": "",
                "pps_id": "RIL-L-8301",
                "destination_location": "",
                "location": "19-038-A",
                "bot_id": "RIL-L-8301",
                "exceptions": []
              },
              "carrier_type": null,
              "carrier_sub_type": null
            }
          ],
          "expectations": {
            "containers": [
              {
                "id": 27254,
                "state": "created",
                "type": "VIRTUAL",
                "barcode": null,
                "containers": [],
                "actions": [],
                "createdOn": "2026-02-25T12:14:28.282Z",
                "updatedOn": "2026-02-25T12:14:28.282Z",
                "transactionId": null,
                "products": [
                  {
                    "id": 26879,
                    "uid": null,
                    "possibleUids": [
                      {
                        "pdfa_values": ["product_sku"],
                        "quantity_per_unit": 1,
                        "product_uid": "2102",
                        "relative_priority": 1,
                        "barcode_map": {"Item": ["10840243112927"]}
                      }
                    ],
                    "productQuantity": 10,
                    "productAttributes": {
                      "barcodes": ["10840243112927"],
                      "product_sku": "10840243112927",
                      "package_parameters": ["package_name = 'Item'"]
                    }
                  }
                ],
                "containerAttributes": null,
                "carrier_type": null,
                "carrier_sub_type": null
              }
            ]
          },
          "exceptions": [],
          "receivedOn": "2026-02-25T12:14:28.281Z",
          "status": "PROCESSED",
          "state": "complete",
          "attributes": {
            "sub_state": "complete",
            "orderType": "GCUS",
            "extra_info": {"client_task_id": "Kellanova_order_6b67c109fa1e"},
            "location": {
              "displayName": "NB-56-A",
              "fullAddress": "NB-56-A-Zone2",
              "addressFields": {"bay": "56", "side": "R", "zone": "Zone2", "aisle": "NB", "level": "A"}
            },
            "simple_priority": "normal"
          },
          "createdOn": "2026-02-25T12:14:28.281Z",
          "updatedOn": "2026-02-25T12:14:28.281Z",
          "isDeleted": false,
          "stages": [
            {
              "id": 11648,
              "orderId": 1816446,
              "externalServiceRequestId": "Kellanova_order999977132_1",
              "transactionStatus": "PROCESSED",
              "transactionState": "complete",
              "transactionType": "Each Pick",
              "childSR": [1816456],
              "ordering": 1
            }
          ],
          "onHold": false
        }
      ],
      "type": "PICK",
      "actuals": {"containers": []},
      "transactions": [],
      "expectations": {"containers": []},
      "exceptions": [],
      "receivedOn": "2026-02-25T12:14:28.280Z",
      "status": "PROCESSING",
      "state": "pick_transaction",
      "attributes": {
        "event_type": "pick_transaction",
        "sub_state": "in_progress",
        "cust_identity": "WALMART",
        "order_options": {
          "destination": "DOCK_DOOR2",
          "palletization": true,
          "container_type": "Type3",
          "simple_priority": "normal",
          "destination_group": "NORTH"
        },
        "destination": "DOCK_DOOR2",
        "flow_name": "default",
        "has_parent": false,
        "simple_priority": "normal"
      },
      "createdOn": "2026-02-25T12:14:28.280Z",
      "updatedOn": "2026-02-25T12:14:33.036Z",
      "isDeleted": false,
      "stages": [],
      "fulfillmentArea": ["assist_area"],
      "onHold": false
    }
  }
}
KAFKA_EOF
```

**Verify:**

```bash
# transaction_status row created
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status FROM transaction_status;"

# outbox_event for item_picked.events PENDING → PUBLISHED
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status FROM outbox_event ORDER BY created_at DESC LIMIT 5;"

# Kafka UI: check gor.item_picked.events → http://localhost:8080
```

**Expected `gor.item_picked.events` payload:**

```json
{
  "pps_id": "8301",
  "seat_name": "seat_1",
  "order_id": "Kellanova_order999977132",
  "slot_ref": "R002.1.3.2",
  "pps_bin_id": "pps_bin_001",
  "transaction_id": "1772445745488",
  "state": "complete",
  "pps_point": "PPS-POINT-1",
  "user_logged_in": "grey7",
  "dangling_area": "bot",
  "picked_item_info_list": [
    {
      "tpid": "2102",
      "item_uid": "10840243112927",
      "uom": "Item",
      "picked_qty": 10,
      "pick_instruction_ids": ["Kellanova_order999977132"]
    }
  ]
}
```

> `dangling_area` = `"bot"` is set automatically for all `pick_transaction` events.

---

## Step 4 — Order Complete (update event)

**What happens:** AE publishes a final `update` event with `state=complete` and `sub_state=complete`.
`PickListEventListener` sends command `COMPLETE` → workflow reaches end state.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-list.events
{
  "headers": {
    "event_type": "update"
  },
  "value": {
    "name": "order_information",
    "payload": {
      "id": 12761,
      "externalServiceRequestId": "Kellanova_order999977132",
      "serviceRequests": [
        {
          "id": 12762,
          "externalServiceRequestId": "Kellanova_order999977132_1",
          "serviceRequests": [],
          "type": "PICK_LINE",
          "actuals": {
            "containers": [
              {
                "id": 2389529,
                "state": "complete",
                "type": "VIRTUAL",
                "barcode": null,
                "containers": [],
                "actions": [],
                "transactionId": "1772445745488",
                "products": [
                  {
                    "id": 2389907,
                    "uid": "867",
                    "productQuantity": 10,
                    "productAttributes": {
                      "tote_id": "C2554495756DOCK_DOOR204",
                      "package_count": 10,
                      "pdfa_values": {"product_sku": "10840243142832"},
                      "package_name": "undefined"
                    }
                  }
                ],
                "containerAttributes": {
                  "internal_order_id": 24234242,
                  "lpn_id": 24234242,
                  "qty_to_be_picked": 10,
                  "qty_picked": 10,
                  "status": "complete",
                  "tote_id": "C2554495756DOCK_DOOR204",
                  "pps_bin_id": "undefined",
                  "user_name": "grey7",
                  "pps_id": "RIL-L-8301",
                  "bot_id": "RIL-L-8301",
                  "location": "19-038-A"
                },
                "carrier_type": null,
                "carrier_sub_type": null
              }
            ]
          },
          "transactions": [],
          "exceptions": [],
          "receivedOn": "2026-02-25T12:14:28.281Z",
          "status": "PROCESSED",
          "state": "complete",
          "attributes": {
            "sub_state": "complete",
            "orderType": "GCUS",
            "extra_info": {"client_task_id": "Kellanova_order_6b67c109fa1e"},
            "simple_priority": "normal"
          },
          "isDeleted": false,
          "stages": [
            {
              "id": 11648,
              "orderId": 1816446,
              "externalServiceRequestId": "Kellanova_order999977132_1",
              "transactionStatus": "PROCESSED",
              "transactionState": "complete",
              "transactionType": "Each Pick",
              "childSR": [1816456],
              "ordering": 1
            }
          ],
          "onHold": false
        }
      ],
      "type": "PICK",
      "actuals": {"containers": []},
      "transactions": [],
      "exceptions": [],
      "receivedOn": "2026-02-25T12:14:28.280Z",
      "status": "PROCESSED",
      "state": "complete",
      "attributes": {
        "event_type": "update",
        "sub_state": "complete",
        "cust_identity": "WALMART",
        "order_options": {
          "destination": "DOCK_DOOR2",
          "palletization": true,
          "container_type": "Type3",
          "simple_priority": "normal",
          "destination_group": "NORTH"
        },
        "destination": "DOCK_DOOR2",
        "flow_name": "default",
        "has_parent": false,
        "simple_priority": "normal"
      },
      "isDeleted": false,
      "stages": [],
      "fulfillmentArea": ["assist_area"],
      "onHold": false
    }
  }
}
KAFKA_EOF
```

**Verify:**

```bash
# ae_order status should be COMPLETE
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT order_id, status FROM ae_order WHERE order_id='Kellanova_order999977132';"

# Camunda process instance ended
docker compose logs spring-camunda | grep -i "workflow complete\|process ended\|Kellanova_order999977132"
```

---

## Useful Queries

```bash
# Full ae_order row
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT * FROM ae_order WHERE order_id='Kellanova_order999977132';"

# All outbox events
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT id, aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at;"

# All transaction statuses
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c "SELECT * FROM transaction_status;"

# Active Camunda process instances
docker exec pick-morph-camunda-postgres-1 \
  psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_ FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 5;"

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

| Topic | Direction | Triggered in |
|-------|-----------|--------------|
| `gor.pick-list.requests` | Outbound (AA → AE) | Step 1 — workflow start |
| `gor.pick-list.response` | Inbound (AE → AA) | Step 2 — AE accepts/rejects |
| `gor.pick-list.events` | Inbound (AE → AA) | Steps 3 & 4 — pick_transaction, update |
| `gor.item_picked.events` | Outbound (AA → Butler Core) | Step 3 — per pick_transaction |
