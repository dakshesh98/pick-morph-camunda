# pick-morph-camunda — Claude Instructions

## Compile the Project (Locally)
Compile the project after every code change made locally
```bash
./mvnw compile -q
```

## Start the app (Docker)

Clean state + rebuild + run (hot path if containers already up, cold start otherwise):
```bash
./setup.sh
```

This script:
- If `spring-camunda` is **already running**: truncates Postgres tables, deletes Kafka topics, rebuilds and restarts only `spring-camunda`
- If **not running**: starts all services (postgres, kafka, kafka-ui, spring-camunda, nginx) from scratch

App is available at `http://localhost:9191` once healthy.

## Rule: keep openapi.yaml in sync with API/Kafka contracts

**Whenever any of the following change**, update `src/main/resources/openapi.yaml` to reflect the new contract:

- REST endpoint request/response payloads (controllers under `controller/`)
- Kafka inbound message DTOs (listeners under `downstream/listener/`)
- Kafka outbound message DTOs (e.g. `PickListRequest`, `ItemPickedEvent`)
- Any DTO under `dto/` that is part of a public API or Kafka topic

What to update in `openapi.yaml`:
- `components/schemas` — add, remove, or modify schema fields to match the DTO's `@JsonProperty` names
- `x-kafka.topics` — update payload `$ref` or inline schema if topic contract changes
- `paths` — update request/response schemas if REST endpoint signatures change

---

## BPMN Viewer

The BPMN viewer lives at `.claude/bpmn-viewer/index.html` and is served via `python3 -m http.server 5500` (configured in `../.claude/launch.json`).

### Rule: update the BPMN viewer whenever delegates change

**Whenever any file under `src/main/java/**/delegates/*.java` is created, modified, or deleted**, update the embedded BPMN XML inside `.claude/bpmn-viewer/index.html` to reflect the current workflow state:

- Add, remove, or rename `<serviceTask>` elements to match the delegate set
- Update `camunda:delegateExpression` values to match current bean names
- Add/remove sequence flows and gateways if the routing logic changed
- Keep task labels concise (use `&#10;` for line breaks in `name` attributes)
- Keep BPMNDI shape coordinates consistent with the existing layout grid

The viewer uses bpmn-js v17 loaded from CDN. The BPMN XML is stored in `<script type="text/bpmn" id="bpmn-xml">` to avoid template-literal interpolation of `${...}` expressions.
