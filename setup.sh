#!/usr/bin/env bash
# ============================================================
# setup.sh — Start / reset pick-morph-camunda for a clean run
#
# HOT path  (spring-camunda already running):
#   1. Truncate app tables (outbox_event, transaction_status, ae_order)
#   2. Truncate Camunda runtime + history tables
#   3. Delete Kafka topics
#   4. Rebuild & restart ONLY spring-camunda (postgres/kafka untouched)
#
# COLD path (nothing running):
#   docker compose down -v  →  docker compose up --build -d
# ============================================================

set -euo pipefail

PROJECT="pick-morph-camunda"
POSTGRES="${PROJECT}-postgres-1"
KAFKA="${PROJECT}-kafka-1"
SPRING="${PROJECT}-spring-camunda-1"

TENANT="${KAFKA_TENANT_PREFIX:-gor}"

KAFKA_TOPICS=(
  "${TENANT}.pick-instruction.events"
  "${TENANT}.pick-instruction.requests"
  "${TENANT}.pick-instruction.response"
  "${TENANT}.pick-list.requests"
  "${TENANT}.pick-list.response"
  "${TENANT}.pick-list.events"
  "${TENANT}.item_picked.events"
  "${TENANT}.order_update.events"
  "${TENANT}.transaction.updates"
  "${TENANT}.transaction.events"
  "${TENANT}.workflow.complete.events"
)

# ── helpers ──────────────────────────────────────────────────
SETUP_START=$(date +%s)

log()  { echo "[setup] $*"; }
ok()   { echo "[setup] ✓ $*"; }
warn() { echo "[setup] ⚠ $*"; }

elapsed() {
  local now; now=$(date +%s)
  echo "$((now - SETUP_START))s"
}

step_start() {
  _STEP_START=$(date +%s)
}

step_done() {
  local now; now=$(date +%s)
  echo "[setup]   └─ done in $((now - _STEP_START))s"
}

pg() {
  docker exec "$POSTGRES" psql -U appuser -d app_db -c "$1"
}

wait_healthy() {
  log "Waiting for spring-camunda to be healthy..."
  local attempts=0
  until docker inspect "$SPRING" --format '{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy"; do
    attempts=$((attempts + 1))
    if [ $attempts -ge 36 ]; then      # 36 × 5 s = 3 min timeout
      warn "Timed out waiting for healthy status. Check: docker compose logs spring-camunda"
      exit 1
    fi
    sleep 5
    printf "."
  done
  echo ""
  ok "spring-camunda is healthy. ($(elapsed) total)"
}

# ── main ─────────────────────────────────────────────────────
if docker ps --format '{{.Names}}' | grep -q "^${SPRING}$"; then

  log "spring-camunda is RUNNING → hot cleanup"

  # 1. Truncate app tables
  step_start
  log "Truncating app tables..."
  pg "TRUNCATE TABLE outbox_event, transaction_status, ae_orders_mapping, ae_order RESTART IDENTITY CASCADE;"
  ok "App tables cleared."
  step_done

  # 2. Truncate Camunda runtime & history tables
  #    Order matters: children before parents to satisfy FK constraints
  step_start
  log "Truncating Camunda runtime/history tables..."
  pg "
    TRUNCATE TABLE
      act_hi_job_log,
      act_hi_ext_task_log,
      act_hi_op_log,
      act_hi_detail,
      act_hi_comment,
      act_hi_attachment,
      act_hi_identitylink,
      act_hi_varinst,
      act_hi_taskinst,
      act_hi_actinst,
      act_hi_dec_in,
      act_hi_dec_out,
      act_hi_decinst,
      act_hi_caseactinst,
      act_hi_caseinst,
      act_hi_incident,
      act_hi_batch,
      act_hi_procinst,
      act_ru_ext_task,
      act_ru_batch,
      act_ru_incident,
      act_ru_identitylink,
      act_ru_task,
      act_ru_variable,
      act_ru_event_subscr,
      act_ru_job,
      act_ru_case_sentry_part,
      act_ru_case_execution,
      act_ru_execution,
      act_ru_meter_log,
      act_ru_task_meter_log
    CASCADE;
  "
  ok "Camunda tables cleared."
  step_done

  # 3. Delete Kafka topics
  step_start
  log "Deleting Kafka topics..."
  for topic in "${KAFKA_TOPICS[@]}"; do
    if docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -qx "$topic"; then
      docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete --topic "$topic"
      ok "Deleted topic: $topic"
    else
      warn "Topic not found (skipped): $topic"
    fi
  done
  step_done

  # 4. Rebuild & restart only spring-camunda
  step_start
  log "Rebuilding spring-camunda image..."
  docker compose build spring-camunda
  log "Restarting spring-camunda container..."
  docker compose stop spring-camunda
  docker compose up -d spring-camunda
  step_done

  wait_healthy

else
  log "spring-camunda is NOT running → cold start"

  step_start
  docker compose down -v
  step_done

  step_start
  log "Starting all services..."
  docker compose up --build -d
  step_done

  log "Tailing logs (Ctrl+C once you see 'Started SpringCamundaApplication')..."
  docker compose logs -f spring-camunda
  log "Cold start complete. ($(elapsed) total)"

fi
