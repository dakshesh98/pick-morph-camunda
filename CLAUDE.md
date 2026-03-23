# pick-morph-camunda — Claude Instructions

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
