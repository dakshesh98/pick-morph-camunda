# Pick-Morph-Camunda — Kellanova End-to-End Test Guide

Tests the full pick instruction workflow using **production-like Kellanova payloads** from the
[LLD: Pick AE Order Creator](https://greyorange-work.atlassian.net/wiki/spaces/BS/pages/134348801/LLD+Pick+AE+Order+Creator).

> **IMPORTANT — For AI assistants executing this guide:**
> Before executing any step (including Step 0 / setup), **always pause and ask the user for confirmation**.
> Show what you are about to run and wait for an explicit "yes" / "go ahead" before proceeding.
> Never run multiple steps back-to-back without user approval between each one.

---

## Test Data Reference

| Field | Value | Source |
|-------|-------|--------|
| `pickInstructionId` | `Kellanova_order999977132` | Root `externalServiceRequestId` in AE order |
| `orderlineId` | `Kellanova_order999977132_1` | serviceRequest `externalServiceRequestId` |
| `transactionId` | `1772445745488` | `transactions[].transactionId` in pick_transaction event |
| `internal_order_id` | `24234242` | `containerAttributes.internal_order_id` |
| `product_sku` | `10840243112927` | `productAttributes.product_sku` (expectations) |
| `product_uid` | `867` | `actuals.containers.products[].uid` |
| `qty` | `10` | `productQuantity` |
| `bot_id / pps_id` | `RIL-L-8301` | `containerAttributes.bot_id / pps_id` |
| `user_name` | `grey7` | `containerAttributes.user_name` |

---

## 0. Setup / Clean Start

```bash
./setup.sh
```

### What it does

| Scenario | Actions |
|----------|---------|
| **spring-camunda is running** (hot) | ① Truncate `ae_order`, `transaction_status`, `outbox_event` <br>② Truncate Camunda runtime/history tables (`act_ru_*`, `act_hi_*`) <br>③ Delete Kafka topics <br>④ Rebuild & restart **only** `spring-camunda` (postgres/kafka untouched) |
| **Nothing is running** (cold) | `docker compose down -v` → `docker compose up --build -d` |

> The script waits for the health check to pass before exiting — safe to immediately run Step 1 once it completes.

---

## Step 1 — Start Workflow (pick instruction received)

Pick instructions can be triggered via **Kafka** (`gor.pick-instruction.events`) or **REST** (kept for testing).

**Option A — Kafka :**

```bash
echo '{"id":"Kellanova_order999977132","order_id":"O123","orderline_id":"O123-OL123","qty":10,"slot_id":"NB-56-A","uom":"Item","tpid":"2102","item_id":"2102","pps_id":8301,"bin_id":"pps_bin_001","extra_fields":{"barcodes":["10840243112927"],"product_sku":"10840243112927","seat_name":"seat_1"}}' | \
  docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic gor.pick-instruction.events
```

**Option B — REST (testing convenience):**

```bash
curl -s -X POST http://localhost:9191/Order/pick_instruction \
  -H "Content-Type: application/json" \
  -d '{
    "id": "Kellanova_order999977132",
    "order_id": "O123",
    "orderline_id": "O123-OL123",
    "qty": 10,
    "slot_id": "NB-56-A",
    "uom": "Item",
    "tpid": "2102",
    "item_id": "2102",
    "pps_id": 8301,
    "bin_id": "pps_bin_001",
    "extra_fields": {
      "barcodes": ["10840243112927"],
      "product_sku": "10840243112927",
      "seat_name": "seat_1"
    }
  }'
```

**Verify:**

```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 15s 2>&1 | \
  grep -E "Kellanova_order999977132|Persisted ae_order|outbox|PUBLISHED"
# Expected:
#   "Pick instruction process started for pickInstructionId: Kellanova_order999977132"
#   "Persisted ae_order + outbox event for pickInstructionId: Kellanova_order999977132"
#   "Outbox PUBLISHED: ... topic=gor.pick-list.requests aggregateId=Kellanova_order999977132"

# 1. ae_order row created with initial payload
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT external_service_request_id, created_at,
          payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->'expectations'->'containers'->0->'products'->0->'productAttributes'->>'product_sku' AS expectations_sku
   FROM ae_order;"
# Expected: external_service_request_id=Kellanova_order999977132, expectations_sku=10840243112927

# 2. outbox_event for pick-list.requests created and published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at DESC LIMIT 3;"
# Expected: topic=gor.pick-list.requests, status=PUBLISHED

# 3. Camunda process instance started
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 1;"
# Expected: business_key_=Kellanova_order999977132, end_time_=null (still running)
```

---

## Step 2 — AE Accepts Pick Request

**Topic:** `gor.pick-list.response`

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

**Failure scenario (AE rejects — workflow aborts):**

```bash
echo '{
  "externalServiceRequestId": "Kellanova_order999977132",
  "serviceRequestId": null,
  "executionId": null,
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
# Camunda message correlated in logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | grep -i "Kellanova_order999977132"
# Expect: "PickListResponseMessage correlated successfully"

# Camunda process still running (not ended)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, end_time_
   FROM act_hi_procinst WHERE business_key_='Kellanova_order999977132';"
# Expected: end_time_=null
```

---

## Step 3a — Intermediate Order Update Events

> All intermediate events are `event_type=update` on topic `gor.pick-list.events`.
> Send them in order before Step 3b (pick_transaction).

### 3a-1 · cancellation_locked (sub_state=created)

When the order can no longer be cancelled. sub_state remains `created`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"CREATED","state":"created","attributes":{"sub_state":"created","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"created","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: cancellation_locked | sub_state: created"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: cancellation_locked | sub_state: created"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: cancellation_locked"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: cancellation_locked | sub_state: created"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->>'state' AS sr_state,
          payload::jsonb->'serviceRequests'->0->>'status' AS sr_status,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=cancellation_locked, sub_state=created, sr_state=created
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=cancellation_locked, sub_state=created, transaction_id=Kellanova_order999977132
```

---

### 3a-2 · in_palletization

Palletization has begun for the order.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"CREATED","state":"created","attributes":{"sub_state":"in_palletization","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"in_palletization","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: cancellation_locked | sub_state: in_palletization"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: cancellation_locked | sub_state: in_palletization"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: cancellation_locked"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: cancellation_locked | sub_state: in_palletization"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=cancellation_locked, sub_state=in_palletization
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=cancellation_locked, sub_state=in_palletization, transaction_id=Kellanova_order999977132
```

---

### 3a-3 · partially_palletized

Some orderlines have actuals (items palletized) but not all. actuals.containers populated with `qty_picked=0`, `status=created`.

**Expected state hierarchy:**
```
Order [Kellanova_order999977132]
├── state:     cancellation_locked
├── sub_state: partially_palletized
│
└── serviceRequests[0]: Kellanova_orderline999977123_1
    ├── state:     created
    ├── sub_state: partially_palletized
    │
    ├── expectations.containers[0]
    │   └── products[0]
    │       ├── productQuantity: 10             ← total expected qty for this OL
    │       └── product_sku:     10840243112927
    │
    └── actuals.containers[0]  (transactionId: 1772445745488)
        ├── status:           created
        ├── qty_to_be_picked: 1                 ← only 1 of N orderlines palletized so far
        └── qty_picked:       0                 ← not yet picked
```

> Root `actuals.containers` is `[]` — the order root never carries container actuals directly.
> OL `state=created` (not `fulfillable`) — mission not yet assigned at this point.
> `qty_to_be_picked (1) < productQuantity (10)` confirms partial palletization — not all expected qty has a container assigned yet.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}]},"transactions":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":1,"qty_picked":0,"status":"created","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"CREATED","state":"created","attributes":{"sub_state":"partially_palletized","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"partially_palletized","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: cancellation_locked | sub_state: partially_palletized"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: cancellation_locked | sub_state: partially_palletized"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: cancellation_locked"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: cancellation_locked | sub_state: partially_palletized"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=cancellation_locked, sub_state=partially_palletized, container_status=created, qty_picked=0
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=cancellation_locked, sub_state=partially_palletized, transaction_id=Kellanova_order999977132_24234242
```

---

### 3a-4 · fully_palletized

All orderlines have actuals. `qty_to_be_picked=10`, `qty_picked=0`, `status=created`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"created","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}]},"transactions":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"created","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"CREATED","state":"created","attributes":{"sub_state":"fully_palletized","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"fully_palletized","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: cancellation_locked | sub_state: fully_palletized"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: cancellation_locked | sub_state: fully_palletized"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: cancellation_locked"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: cancellation_locked | sub_state: fully_palletized"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_to_be_picked' AS qty_to_pick,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=cancellation_locked, sub_state=fully_palletized, qty_to_pick=10, qty_picked=0
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=cancellation_locked, sub_state=fully_palletized, transaction_id=Kellanova_order999977132_24234242
```

---

### 3a-5 · internal_order_created

Internal order created in SRMS. Terminal state for customer order process flow. `transactions` field is empty (ignored by ae_order update logic).

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":0,"status":"created","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}]},"transactions":[],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"CREATED","state":"created","attributes":{"sub_state":"internal_order_created","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"cancellation_locked","attributes":{"event_type":"update","sub_state":"INTERNAL_ORDER_CREATED","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: cancellation_locked | sub_state: INTERNAL_ORDER_CREATED"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: cancellation_locked | sub_state: INTERNAL_ORDER_CREATED"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: cancellation_locked"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: cancellation_locked | sub_state: INTERNAL_ORDER_CREATED"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=cancellation_locked, sub_state=INTERNAL_ORDER_CREATED, container_status=created
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=cancellation_locked, sub_state=INTERNAL_ORDER_CREATED, transaction_id=Kellanova_order999977132_24234242
```

---

### 3a-6 · fullfillable / in_progress (pallet loaded)

Internal order pallet loaded onto bot. `state=fulfillable`, `status=loaded`, `qty_picked=0`.

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_orderline999977123_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":"52342","lpn_id":"52342","qty_to_be_picked":10,"qty_picked":0,"status":"loaded","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}]},"transactions":[{"transactionId":"1772445745488","containerAttributes":{"internal_order_id":"52342","lpn_id":"52342","qty_to_be_picked":10,"qty_picked":0,"status":"loaded","bot_id":"RIL-L-8305","pps_id":"RIL-L-8305","exceptions":[]}}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"PROCESSING","state":"fulfillable","attributes":{"sub_state":"in_progress","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","state":"fulfillable","attributes":{"event_type":"update","sub_state":"in_progress","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**
```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|Enqueued OrderUpdateEvent"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: fulfillable | sub_state: in_progress"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: fulfillable | sub_state: in_progress"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: fulfillable"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: ..."
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_orderline999977123_1 | state: fulfillable | sub_state: in_progress"

# DB
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state, payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->>'state' AS sr_state,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=fulfillable, sub_state=in_progress, sr_state=fulfillable, container_status=loaded, qty_picked=0
# order_update.events — OrderUpdateEvent published
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=fulfillable, sub_state=in_progress, transaction_id=Kellanova_order999977132_52342
```

---

## Step 3b — Bot Picks Item (pick_transaction event)

**What happens:** AE publishes `pick_transaction`. App saves `transaction_status` (with payload)
+ outbox event atomically → relay publishes to `gor.item_picked.events`.

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"pick_transaction"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_order999977132_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-03-02T10:02:27.987Z","updatedOn":"2026-03-02T10:02:27.989Z","transactionId":"1772445745488","products":[{"id":2389907,"uid":"867","possibleUids":null,"uidType":null,"createdOn":"2026-03-02T10:02:27.988Z","updatedOn":"2026-03-02T10:02:27.988Z","productQuantity":10,"productAttributes":{"tote_id":"C2554495756DOCK_DOOR204","package_count":10,"pdfa_values":{"product_sku":"10840243142832"},"package_name":"undefined","serialized_content":[],"tote_ids":["C2554495756DOCK_DOOR204"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":10,"status":"complete","tote_id":"C2554495756DOCK_DOOR204","pps_bin_id":"undefined","pps_seat_name":"undefined","trueCopyIdOf":1816456,"user_name":"grey7","rollcage_id":"","pps_id":"RIL-L-8301","destination_location":"","location":"19-038-A","bot_id":"RIL-L-8301","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-03-02T10:02:27.987Z","updatedOn":"2026-03-02T10:02:27.989Z","transactionId":"1772445745488","products":[{"id":2389907,"uid":"867","possibleUids":null,"uidType":null,"createdOn":"2026-03-02T10:02:27.988Z","updatedOn":"2026-03-02T10:02:27.988Z","productQuantity":10,"productAttributes":{"tote_id":"C2554495756DOCK_DOOR204","package_count":10,"pdfa_values":{"product_sku":"10840243142832"},"package_name":"undefined","serialized_content":[],"tote_ids":["C2554495756DOCK_DOOR204"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":10,"status":"complete","tote_id":"C2554495756DOCK_DOOR204","pps_bin_id":"undefined","pps_seat_name":"undefined","trueCopyIdOf":1816456,"user_name":"grey7","rollcage_id":"","pps_id":"RIL-L-8301","destination_location":"","location":"19-038-A","bot_id":"RIL-L-8301","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[{"id":11648,"orderId":1816446,"externalServiceRequestId":"Kellanova_order999977132_1","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[1816456],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","status":"PROCESSING","state":"pick_transaction","attributes":{"event_type":"pick_transaction","sub_state":"in_progress","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**

```bash
# Logs
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|pick_transaction|validatePersistAndEnqueue|Enqueued ItemPickedEvent|Enqueued OrderUpdateEvent|Outbox PUBLISHED"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: pick_transaction | state: pick_transaction | sub_state: in_progress"
#   "pick_transaction event | pickInstructionId: Kellanova_order999977132"
#   "SendItemPickedEventDelegate executing for pickInstructionId: Kellanova_order999977132, aeEventType: pick_transaction"
#   "validatePersistAndEnqueue — pickInstructionId: Kellanova_order999977132, txId: 1772445745488"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: pick_transaction"
#   "Enqueued ItemPickedEvent to outbox for pickInstructionId: Kellanova_order999977132, txId: 1772445745488"
#   "ItemPickingEventMessage correlated for pick_transaction | pickInstructionId: Kellanova_order999977132"
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_order999977132_1 | state: pick_transaction | sub_state: in_progress"
#   "Outbox PUBLISHED: id=... topic=gor.item_picked.events aggregateId=Kellanova_order999977132"
```

```bash
# 1. transaction_status row created with full container payload
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT pick_instruction_id, transaction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'qty_to_be_picked' AS qty_to_pick,
          payload::jsonb->'containerAttributes'->>'internal_order_id' AS internal_order_id,
          payload::jsonb->'containerAttributes'->>'bot_id' AS bot_id,
          payload::jsonb->'containerAttributes'->>'status' AS container_status
   FROM transaction_status;"
# Expected: transaction_id=1772445745488, status=SUCCESS, qty_picked=10, internal_order_id=24234242
# Note: transaction_id here is AE's txId; order_update/item_picked events use pick_instruction_id + internal_order_id

# 2. ae_order updated — actuals, expectations and state/sub_state set
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->>'state' AS sr_state,
          payload::jsonb->'serviceRequests'->0->>'status' AS sr_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->>'transactionId' AS actual_tx_id,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'serviceRequests'->0->'expectations'->'containers'->0->'products'->0->'productAttributes'->>'product_sku' AS exp_sku,
          updated_at
   FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=pick_transaction, sr_state=complete, actual_tx_id=1772445745488, qty_picked=10, exp_sku=10840243112927

# 3. outbox_event PUBLISHED for item_picked.events
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at DESC LIMIT 5;"
# Expected: topic=gor.item_picked.events, status=PUBLISHED

# 4. order_update.events — OrderUpdateEvent published (pick_transaction)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, order_id=O123, orderline_id=O123-OL123, state=pick_transaction, sub_state=in_progress, qty_picked=10, transaction_id=Kellanova_order999977132_24234242
```

**Read `gor.item_picked.events` from Kafka:**

```bash
docker exec pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic gor.item_picked.events \
  --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | python3 -m json.tool
```

**Expected `gor.item_picked.events` payload:**

```json
{
  "pps_id": "8301",
  "seat_name": "seat_1",
  "order_id": "Kellanova_order999977132",
  "slot_ref": "R002.1.3.2",
  "pps_bin_id": "pps_bin_001",
  "transaction_id": "Kellanova_order999977132_24234242",
  "state": "complete",
  "pps_point": "PPS-POINT-1",
  "user_logged_in": "grey7",
  "dangling_area": "bot",
  "is_marked_container_flow": false,
  "last_item_picked": false,
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

> `dangling_area = "bot"` is set automatically for all `pick_transaction` events (bot pick flow).

---

## Step 4 — Order Complete (final update event)

**What happens:** AE publishes `update` with `state=complete`. Workflow reaches end state.

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_order999977132_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"transactionId":"1772445745488","products":[{"id":2389907,"uid":"867","productQuantity":10,"productAttributes":{"tote_id":"C2554495756DOCK_DOOR204","package_count":10,"pdfa_values":{"product_sku":"10840243142832"},"package_name":"undefined"}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":10,"status":"complete","tote_id":"C2554495756DOCK_DOOR204","pps_bin_id":"undefined","user_name":"grey7","pps_id":"RIL-L-8301","bot_id":"RIL-L-8301","location":"19-038-A"},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[],"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"isDeleted":false,"stages":[{"id":11648,"orderId":1816446,"externalServiceRequestId":"Kellanova_order999977132_1","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[1816456],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","status":"PROCESSED","state":"complete","attributes":{"event_type":"update","sub_state":"complete","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**Verify:**

```bash
# 1. Logs — update correlated, workflow finalizing and complete
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|ItemPickingEventMessage|COMPLETE|OnWorkflowComplete|Workflow"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: complete | sub_state: complete"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | command: COMPLETE | state: complete | sub_state: complete"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, state: complete"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: COMPLETE"
#   "MarkPickInstructionCompleteDelegate executing for pickInstructionId: Kellanova_order999977132"
#   "Finalizing Workflow: Pick Instruction Kellanova_order999977132 is COMPLETE."
#   "OnWorkflowCompleteDelegate executing for pickInstructionId: Kellanova_order999977132, finalStatus: COMPLETED"
#   "Workflow completing for pickInstructionId: Kellanova_order999977132 — running cleanup and validation."
#   "Workflow cleanup complete for pickInstructionId: Kellanova_order999977132 — final status: COMPLETED"

# 2. ae_order final state — complete
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state,
          payload::jsonb->'serviceRequests'->0->>'state' AS sr_state,
          payload::jsonb->'serviceRequests'->0->>'status' AS sr_status,
          updated_at
   FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: state=complete, sub_state=complete, sr_state=complete, sr_status=PROCESSED

# 3. Camunda process instance ended
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst WHERE business_key_='Kellanova_order999977132';"
# Expected: end_time_ is NOT null (process completed)

# 4. All outbox events published (no PENDING remaining)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status, count(*) FROM outbox_event GROUP BY topic, status ORDER BY topic;"
# Expected: all rows have status=PUBLISHED
```

---

## Step 4c — Pallet Unloaded (unloading event → `released`)

**What happens:** The pallet is physically unloaded at the dock door. AE publishes `update` with
`containerAttributes.status = "unloaded"`. The `AEOrderTransformer` sees all containers with
`qty_picked == OL expectation` **and** all containers `unloaded` → computes `ol_status = released`
→ `ae_order.status = released` → Camunda workflow completes automatically.

> **LLD note (Confluence):** Unloading the pallet releases notifications for all customer order
> updates associated with that pallet. Unloading does **not** necessarily mean that related
> customer orders are complete — some orders may still be in progress. In this single-OL test
> the order is fully released.

**Topic:** `gor.pick-list.events`

```bash
cat << 'KAFKA_EOF' | tr -d '\n' | docker exec -i pick-morph-camunda-kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic gor.pick-list.events
{"headers":{"event_type":"update"},"value":{"name":"order_information","payload":{"id":12761,"externalServiceRequestId":"Kellanova_order999977132","serviceRequests":[{"id":12762,"externalServiceRequestId":"Kellanova_order999977132_1","serviceRequests":[],"type":"PICK_LINE","actuals":{"containers":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-03-02T10:02:27.987Z","updatedOn":"2026-03-02T10:02:27.989Z","transactionId":"1772445745488","products":[{"id":2389907,"uid":"867","possibleUids":null,"uidType":null,"createdOn":"2026-03-02T10:02:27.988Z","updatedOn":"2026-03-02T10:02:27.988Z","productQuantity":10,"productAttributes":{"tote_id":"C2554495756DOCK_DOOR204","package_count":10,"pdfa_values":{"product_sku":"10840243142832"},"package_name":"undefined","serialized_content":[],"tote_ids":["C2554495756DOCK_DOOR204"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":10,"status":"unloaded","tote_id":"C2554495756DOCK_DOOR204","pps_bin_id":"undefined","pps_seat_name":"undefined","trueCopyIdOf":1816456,"user_name":"grey7","rollcage_id":"","pps_id":"RIL-L-8301","destination_location":"","location":"19-038-A","bot_id":"RIL-L-8301","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}]},"transactions":[{"id":2389529,"state":"complete","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-03-02T10:02:27.987Z","updatedOn":"2026-03-02T10:02:27.989Z","transactionId":"1772445745488","products":[{"id":2389907,"uid":"867","possibleUids":null,"uidType":null,"createdOn":"2026-03-02T10:02:27.988Z","updatedOn":"2026-03-02T10:02:27.988Z","productQuantity":10,"productAttributes":{"tote_id":"C2554495756DOCK_DOOR204","package_count":10,"pdfa_values":{"product_sku":"10840243142832"},"package_name":"undefined","serialized_content":[],"tote_ids":["C2554495756DOCK_DOOR204"]}}],"containerAttributes":{"internal_order_id":24234242,"lpn_id":24234242,"qty_to_be_picked":10,"qty_picked":10,"status":"unloaded","tote_id":"C2554495756DOCK_DOOR204","pps_bin_id":"undefined","pps_seat_name":"undefined","trueCopyIdOf":1816456,"user_name":"grey7","rollcage_id":"","pps_id":"RIL-L-8301","destination_location":"","location":"19-038-A","bot_id":"RIL-L-8301","exceptions":[]},"carrier_type":null,"carrier_sub_type":null}],"expectations":{"containers":[{"id":27254,"state":"created","type":"VIRTUAL","barcode":null,"containers":[],"actions":[],"createdOn":"2026-02-25T12:14:28.282Z","updatedOn":"2026-02-25T12:14:28.282Z","transactionId":null,"products":[{"id":26879,"uid":null,"possibleUids":[{"pdfa_values":["product_sku"],"quantity_per_unit":1,"product_uid":"2102","relative_priority":1,"barcode_map":{"Item":["10840243112927"]}}],"uidType":null,"createdOn":"2026-02-25T12:14:28.283Z","updatedOn":"2026-02-25T12:14:28.283Z","productQuantity":10,"productAttributes":{"filter_parameters":["product_sku = '10840243112927'"],"weight":{"uom":{"id":1,"type":"WEIGHT","unit":"KG","conversionFactor":1},"value":4.264},"description":"client_item","orientation_preference":true,"barcodes":["10840243112927"],"display_name":"10840243112927","tag_strategy":{"tag_name":"expirationDate","direction":"ASC"},"package_count":10,"product_sku":"10840243112927","stacking_sequence":5,"package_parameters":["package_name = 'Item'"],"fragile":false,"tag_parameters":[{"operator":"in","tag_name":"status","tag_value":["EXTN","CRIT","GOOD"]},{"operator":">","tag_name":"expirationDate","tag_value":"2026-02-25T14:14:28Z"}],"dimension":{"uom":{"id":51,"type":"DIMENSION","unit":"CM","conversionFactor":1},"width":22.353,"height":10.414,"length":30.48}}}],"containerAttributes":null,"carrier_type":null,"carrier_sub_type":null}]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.281Z","status":"PROCESSED","state":"complete","attributes":{"sub_state":"complete","orderType":"GCUS","sr_parentsIds":[12761],"extra_info":{"client_task_id":"Kellanova_order_6b67c109fa1e"},"shelfLifeHours":2,"sr_parent":12761,"has_parent":true,"location":{"displayName":"NB-56-A","fullAddress":"NB-56-A-Zone2","addressFields":{"bay":"56","side":"R","zone":"Zone2","aisle":"NB","level":"A"}},"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z"},"createdOn":"2026-02-25T12:14:28.281Z","updatedOn":"2026-02-25T12:14:28.281Z","isDeleted":false,"stages":[{"id":11648,"orderId":1816446,"externalServiceRequestId":"Kellanova_order999977132_1","transactionStatus":"PROCESSED","transactionState":"complete","transactionType":"Each Pick","childSR":[1816456],"ordering":1}],"onHold":false}],"type":"PICK","actuals":{"containers":[]},"transactions":[],"expectations":{"containers":[]},"exceptions":[],"receivedOn":"2026-02-25T12:14:28.280Z","status":"PROCESSED","state":"complete","attributes":{"event_type":"update","sub_state":"complete","cust_identity":"WALMART","order_options":{"bintags":[],"behaviours":["group_by_proximity"],"destination":"DOCK_DOOR2","clubbing_key":"WALMART_Type3_NORTH","cust_identity":"WALMART","palletization":true,"container_type":"Type3","order_clubbing":true,"order_splitting":true,"simple_priority":"normal","pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true,"destination_group":"NORTH","orderline_clubbing":true,"orderline_splitting":true},"destination":"DOCK_DOOR2","flow_name":"default","has_parent":false,"simple_priority":"normal","allowed_storage_system_types":["rtp"],"pick_before_time":"2026-02-25T22:55:52Z","clubbing_required":true},"createdOn":"2026-02-25T12:14:28.280Z","updatedOn":"2026-02-25T12:14:33.036Z","isDeleted":false,"stages":[],"fulfillmentArea":["assist_area"],"onHold":false}}}
KAFKA_EOF
```

**State hierarchy after this event:**

```
Order: Kellanova_order999977132
├── ae_order.state      = complete          (AE root state)
├── ae_order.status     = released          ← derived by AEOrderTransformer (NEW)
└── OL: Kellanova_order999977132_1
    ├── sr.state        = complete
    ├── ol_status       = released          ← all containers unloaded + qty_picked == expectation
    └── Container[0]
        ├── status            = unloaded    ← KEY CHANGE from "complete"
        ├── qty_to_be_picked  = 10
        └── qty_picked        = 10
```

**Verify:**

```bash
# Logs — unloading correlated, status=released, workflow completing
docker logs pick-morph-camunda-spring-camunda-1 --since 10s 2>&1 | \
  grep -E "Kellanova_order999977132|Updated ae_order|released|ItemPickingEventMessage|Enqueued OrderUpdateEvent|Finalizing|OnWorkflowComplete|Workflow"
# Expected:
#   "Received pick-list event from AE | pickInstructionId: Kellanova_order999977132 | event_type: update | state: complete | sub_state: complete"
#   "AE update event | pickInstructionId: Kellanova_order999977132 | ... | state: complete | sub_state: complete"
#   "Status injected — pickInstructionId: Kellanova_order999977132, orderStatus: released"
#   "Updated ae_order for pickInstructionId: Kellanova_order999977132, aeState: complete, orderStatus: released"
#   "ItemPickingEventMessage correlated for update event | pickInstructionId: Kellanova_order999977132 | command: COMPLETE"
#   "Enqueued OrderUpdateEvent | pickInstructionId: Kellanova_order999977132 | orderline: Kellanova_order999977132_1 | state: complete | sub_state: complete"
#   "MarkPickInstructionCompleteDelegate executing for pickInstructionId: Kellanova_order999977132"
#   "Finalizing Workflow: Pick Instruction Kellanova_order999977132 is COMPLETE."
#   "OnWorkflowCompleteDelegate executing for pickInstructionId: Kellanova_order999977132, finalStatus: COMPLETED"
#   "Workflow cleanup complete for pickInstructionId: Kellanova_order999977132 — final status: COMPLETED"
```

```bash
# 1. ae_order — status=released, state=complete
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT payload::jsonb->>'state' AS ae_state,
          status,
          state,
          payload::jsonb->'serviceRequests'->0->>'status' AS ol_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'status' AS container_status,
          payload::jsonb->'serviceRequests'->0->'actuals'->'containers'->0->'containerAttributes'->>'qty_picked' AS qty_picked,
          updated_at
   FROM ae_order WHERE external_service_request_id='Kellanova_order999977132';"
# Expected: ae_state=complete, status=released, state=complete, ol_status=released, container_status=unloaded, qty_picked=10

# 2. Camunda process instance ended
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst WHERE business_key_='Kellanova_order999977132';"
# Expected: end_time_ is NOT null (process completed)

# 3. order_update.events — OrderUpdateEvent published (unloading)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status,
          payload::jsonb->>'order_id' AS order_id,
          payload::jsonb->>'orderline_id' AS orderline_id,
          payload::jsonb->>'state' AS state,
          payload::jsonb->>'sub_state' AS sub_state,
          payload::jsonb->>'transaction_id' AS transaction_id
   FROM outbox_event WHERE topic='gor.order_update.events' ORDER BY created_at DESC LIMIT 1;"
# Expected: status=PUBLISHED, state=complete, sub_state=complete, transaction_id=Kellanova_order999977132_24234242

# 4. All outbox events published (no PENDING remaining)
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT topic, status, count(*) FROM outbox_event GROUP BY topic, status ORDER BY topic;"
# Expected: all rows have status=PUBLISHED
```

---

## AE Order Update Rules (from LLD)

When any `gor.pick-list.events` message is received, **only** these fields are updated in `ae_order`:

| Field | Level |
|-------|-------|
| `actuals` | root + serviceRequests |
| `expectations` | root + serviceRequests |
| `exceptions` | root + serviceRequests |
| `attributes` | root + serviceRequests |
| `state` | root + serviceRequests |
| `status` | root + serviceRequests |
| `sub_state` (inside `attributes`) | root + serviceRequests |

> `transactions` field is **ignored** — never written to `ae_order`.
> All other fields (e.g. `type`, `createdOn`, `stages`) are **never overwritten**.

### State Machine Reference

| Scenario | Order State | OL State | Order sub_state | OL sub_state |
|----------|------------|----------|-----------------|--------------|
| Order created | `created` | `created` | `created` | `created` |
| Before palletization | `cancellation_locked` | `created` | `created` | `created` |
| Sent to palletization | `cancellation_locked` | `created` | `in_palletization` | `in_palletization` |
| All pallets created | `cancellation_locked` | `created` | `fully_palletized` | `fully_palletized` |
| Internal orders created in SRMS | `cancellation_locked` | `created` | `internal_order_created` | `internal_order_created` |
| Sent for picking (mission) | `fullfillable` | `fullfillable` | `in_progress` | `in_progress` |
| Partial pallet picked | `pick_transaction` | `fullfillable` | `in_progress` | `in_progress` |
| All containers of OL picked | `pick_transaction` | `complete` | `in_progress` | `complete` |
| All OLs of order complete | `complete` | `complete` | `complete` | `complete` |
| All containers unloaded at dock | `complete` | `complete` | `complete` | `complete` |

> **`ae_order.status` (derived by AEOrderTransformer):** `created` → `pending` → `complete` → `released`
> `released` is set when all containers across all OLs have `status=unloaded` **and** `qty_picked == OL expectation`.
> When `ae_order.status = released`, the Camunda workflow completes automatically.

---

## Useful Queries

```bash
# Full ae_order payload
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT external_service_request_id, updated_at,
          payload::jsonb->>'state' AS state,
          payload::jsonb->'attributes'->>'sub_state' AS sub_state
   FROM ae_order;"

# transaction_status with transaction payload
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT transaction_id, pick_instruction_id, status,
          payload::jsonb->'containerAttributes'->>'qty_picked' AS qty_picked,
          payload::jsonb->'containerAttributes'->>'internal_order_id' AS internal_order_id,
          payload::jsonb->'containerAttributes'->>'bot_id' AS bot_id
   FROM transaction_status;"

# All outbox events
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT id, aggregate_id, topic, status, published_at FROM outbox_event ORDER BY created_at;"

# Camunda process instances
docker exec pick-morph-camunda-postgres-1 psql -U appuser -d app_db -c \
  "SELECT proc_inst_id_, business_key_, start_time_, end_time_
   FROM act_hi_procinst ORDER BY start_time_ DESC LIMIT 5;"

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
| `gor.pick-instruction.events` | Inbound (Orchestrator → AA) | Step 1 — trigger workflow via Kafka |
| `gor.pick-list.requests` | Outbound (AA → AE) | Step 1 — workflow start |
| `gor.pick-list.response` | Inbound (AE → AA) | Step 2 — AE accepts/rejects |
| `gor.pick-list.events` | Inbound (AE → AA) | Steps 3a, 3b, 4c — update, pick_transaction, unloading |
| `gor.item_picked.events` | Outbound (AA → Butler Core) | Step 3b — per pick_transaction |
| `gor.order_update.events` | Outbound (AA → Butler Core) | Steps 3a, 3b & 4c — every update, pick_transaction + unloading |
