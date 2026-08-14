const form = document.querySelector("#request-form");
const requestInput = document.querySelector("#request-input");
const runButton = document.querySelector("#run-button");
const runtimeVersion = document.querySelector("#runtime-version");
const runStatus = document.querySelector("#run-status");
const eventCount = document.querySelector("#event-count");
const eventList = document.querySelector("#event-list");
const evidenceEmpty = document.querySelector("#evidence-empty");
const resultEmpty = document.querySelector("#result-empty");
const resultContent = document.querySelector("#result-content");
const resultClarification = document.querySelector("#result-clarification");
const resultError = document.querySelector("#result-error");
const resultTitle = document.querySelector("#result-title");
const resultMessage = document.querySelector("#result-message");
const clarificationMessage = document.querySelector("#clarification-message");
const errorMessage = document.querySelector("#error-message");

const evidenceFields = {
  option: document.querySelector("#plan-option"),
  source: document.querySelector("#plan-source"),
  transport_cost: document.querySelector("#cost-transport"),
  hotel_cost: document.querySelector("#cost-hotel"),
  activity_cost: document.querySelector("#cost-activities"),
  total_cost: document.querySelector("#cost-total"),
  cost_weight: document.querySelector("#rank-cost-weight"),
  duration_weight: document.querySelector("#rank-duration-weight"),
};

const planFields = {
  destination: document.querySelector("#plan-destination"),
  days: document.querySelector("#plan-days"),
  total_cost: document.querySelector("#plan-cost"),
  flight_type: document.querySelector("#plan-flight"),
  hotel_tier: document.querySelector("#plan-hotel"),
  poi_style: document.querySelector("#plan-style"),
};

let session = null;
let renderedSequence = 0;
const toolEvidence = new Map();
const plannerArguments = new Map();
const RUN_TIMEOUT_MS = 60_000;
const POLL_INTERVAL_MS = 220;

function delay(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

function randomId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function responseJson(response) {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = typeof body.detail === "string" ? body.detail : response.statusText;
    throw new Error(detail || `Request failed with status ${response.status}`);
  }
  return body;
}

async function runtimeFetch(path, options = {}) {
  if (!session) {
    throw new Error("The local demo session is not ready.");
  }
  const headers = new Headers(options.headers || {});
  headers.set("Authorization", `Bearer ${session.api_key}`);
  if (options.body) {
    headers.set("Content-Type", "application/json");
  }
  return responseJson(await fetch(path, { ...options, headers }));
}

function setStatus(status) {
  const normalized = status || "idle";
  const labels = {
    idle: "Ready",
    queued: "Queued",
    running: "Running",
    completed: "Completed",
    failed: "Failed",
    cancelled: "Cancelled",
  };
  runStatus.textContent = labels[normalized] || normalized;
  runStatus.className = `run-status ${normalized}`;
}

function resetWorkspace() {
  renderedSequence = 0;
  toolEvidence.clear();
  plannerArguments.clear();
  eventList.replaceChildren();
  evidenceEmpty.hidden = false;
  eventCount.textContent = "0 events";
  resultEmpty.hidden = false;
  resultContent.hidden = true;
  resultClarification.hidden = true;
  resultError.hidden = true;
  resultTitle.textContent = "Result";
  setStatus("running");
}

function captureEvidence(event) {
  const payload = event.payload || {};
  if (
    event.event_type === "tool.result" &&
    typeof payload.tool_name === "string" &&
    payload.result &&
    typeof payload.result === "object"
  ) {
    toolEvidence.set(payload.tool_name, payload.result);
  }
  const decision = payload.decision || {};
  if (
    event.event_type === "planner.decision" &&
    decision.decision_type === "CALL_TOOL" &&
    typeof decision.tool_name === "string" &&
    decision.arguments &&
    typeof decision.arguments === "object"
  ) {
    plannerArguments.set(decision.tool_name, decision.arguments);
  }
}

function eventSummary(event) {
  const payload = event.payload || {};
  switch (event.event_type) {
    case "run.queued":
      return `${payload.agent_id}:${payload.agent_version} accepted`;
    case "run.started":
      return "Worker claimed durable run";
    case "run.recovered":
      return "Interrupted run recovered";
    case "checkpoint.loaded":
      return `State loaded from ${payload.source || "durable store"}`;
    case "checkpoint.saved":
      return "Updated state committed";
    case "memory.retrieved": {
      const count = Array.isArray(payload.memories) ? payload.memories.length : 0;
      return `${count} governed preference${count === 1 ? "" : "s"} retrieved`;
    }
    case "planner.decision": {
      const decision = payload.decision || {};
      if (decision.decision_type === "CALL_TOOL") {
        return `CALL_TOOL · ${decision.tool_name}`;
      }
      return decision.decision_type || payload.outcome || "Planner decision";
    }
    case "policy.decision":
      return `${payload.outcome || "checked"} · ${payload.tool_name || "tool call"}`;
    case "tool.result":
      return `${payload.tool_name || "registered tool"} · ${payload.status || "recorded"}`;
    case "loop.outcome":
      return payload.outcome || "Loop completed";
    case "run.completed":
      return "Run and checkpoint committed";
    case "run.failed":
      return payload.error_code || "Runtime execution failed";
    case "run.cancelled":
      return payload.reason || "Run cancelled";
    default:
      if (event.event_type.startsWith("external_action.")) {
        return payload.status || payload.outcome || "Durable external action evidence";
      }
      return payload.evidence_id || "Runtime event";
  }
}

function eventClass(event) {
  if (event.event_type === "planner.decision") {
    return "planner";
  }
  if (event.event_type === "policy.decision") {
    return "policy";
  }
  if (event.event_type === "tool.result") {
    return "tool";
  }
  if (event.event_type === "loop.outcome") {
    return "outcome";
  }
  if (event.event_type === "run.completed") {
    return "success";
  }
  if (["run.failed", "run.cancelled"].includes(event.event_type)) {
    return "failure";
  }
  return "";
}

function appendEvent(event) {
  const item = document.createElement("li");
  item.className = `event-item ${eventClass(event)}`.trim();

  const marker = document.createElement("span");
  marker.className = "event-marker";
  marker.setAttribute("aria-hidden", "true");

  const details = document.createElement("details");
  details.className = "event-card";

  const summary = document.createElement("summary");
  const type = document.createElement("span");
  type.className = "event-type";
  type.textContent = event.event_type;

  const description = document.createElement("span");
  description.className = "event-summary";
  description.textContent = eventSummary(event);

  const sequence = document.createElement("span");
  sequence.className = "event-sequence";
  sequence.textContent = `#${String(event.sequence).padStart(2, "0")}`;

  const payload = document.createElement("pre");
  payload.textContent = JSON.stringify(event.payload || {}, null, 2);

  summary.append(sequence, type, description);
  details.append(summary, payload);
  item.append(marker, details);
  eventList.append(item);
}

function renderEvents(events) {
  for (const event of events) {
    if (event.sequence <= renderedSequence) {
      continue;
    }
    captureEvidence(event);
    appendEvent(event);
    renderedSequence = event.sequence;
  }
  const count = eventList.childElementCount;
  evidenceEmpty.hidden = count > 0;
  eventCount.textContent = `${count} event${count === 1 ? "" : "s"}`;
}

function formatSgd(value) {
  const amount = Number(value);
  return Number.isFinite(amount) ? `${amount.toLocaleString()} SGD` : "—";
}

function formatPercent(value) {
  const weight = Number(value);
  return Number.isFinite(weight) ? `${Math.round(weight * 100)}%` : "—";
}

function selectedOptionName(itinerary, ranking) {
  const rankedName = ranking?.ranking?.[0]?.name;
  if (typeof rankedName === "string" && rankedName) {
    return rankedName;
  }
  const selectionNote = itinerary.notes?.find((note) =>
    note.startsWith("Selected option: "),
  );
  return selectionNote?.slice("Selected option: ".length).replace(/\.$/, "") ||
    "Selected reference option";
}

function renderPlanEvidence(itinerary) {
  const search = toolEvidence.get("search_trip_options") || {};
  const ranking = toolEvidence.get("rank_trip_options") || {};
  const route = toolEvidence.get("route_cost_summary") || {};
  const rankArguments = plannerArguments.get("rank_trip_options") || {};
  const optionName = selectedOptionName(itinerary, ranking);
  const selected = Array.isArray(search.options)
    ? search.options.find((option) => option?.name === optionName) || {}
    : {};

  evidenceFields.option.textContent = optionName;
  evidenceFields.source.textContent =
    search.source === "synthetic_reference_catalog"
      ? "Offline synthetic catalog"
      : "Persisted tool evidence";
  evidenceFields.transport_cost.textContent = formatSgd(selected.transport_cost);
  evidenceFields.hotel_cost.textContent = formatSgd(selected.hotel_cost);
  evidenceFields.activity_cost.textContent = formatSgd(selected.activity_cost);
  evidenceFields.total_cost.textContent = formatSgd(
    route.total_cost ?? itinerary.total_cost,
  );
  evidenceFields.cost_weight.textContent = formatPercent(rankArguments.cost_weight);
  evidenceFields.duration_weight.textContent = formatPercent(
    rankArguments.duration_weight,
  );
}

function renderCompletedRun(run) {
  const itinerary = run.state && run.state.itinerary;
  if (!itinerary) {
    showClarification(
      run.output_message || "The Planner needs more information before it can commit a plan.",
    );
    return;
  }
  resultEmpty.hidden = true;
  resultClarification.hidden = true;
  resultError.hidden = true;
  resultContent.hidden = false;
  resultTitle.textContent = "Validated reference plan";
  resultMessage.textContent =
    run.output_message || "A validated synthetic reference plan is ready.";
  planFields.destination.textContent = itinerary.destination;
  planFields.days.textContent = `${itinerary.days} days`;
  planFields.total_cost.textContent = `${Number(itinerary.total_cost).toLocaleString()} SGD`;
  planFields.flight_type.textContent = itinerary.flight_type.replaceAll("_", " ");
  planFields.hotel_tier.textContent = itinerary.hotel_tier;
  planFields.poi_style.textContent = itinerary.poi_style;
  renderPlanEvidence(itinerary);
}

function showClarification(message) {
  resultEmpty.hidden = true;
  resultContent.hidden = true;
  resultError.hidden = true;
  resultClarification.hidden = false;
  resultTitle.textContent = "Clarification required";
  clarificationMessage.textContent = message;
}

function showFailure(message) {
  resultEmpty.hidden = true;
  resultContent.hidden = true;
  resultClarification.hidden = true;
  resultError.hidden = false;
  resultTitle.textContent = "Execution failed";
  errorMessage.textContent = message;
}

// Native EventSource cannot attach the Bearer token required by the Runtime SSE endpoint,
// so the local console uses bounded cursor-based polling over the same persisted event store.
async function pollRun(runId) {
  const deadline = Date.now() + RUN_TIMEOUT_MS;
  const encodedRunId = encodeURIComponent(runId);
  while (Date.now() < deadline) {
    const [run, events] = await Promise.all([
      runtimeFetch(`/runs/${encodedRunId}`),
      runtimeFetch(
        `/runs/${encodedRunId}/events?after_sequence=${renderedSequence}`,
      ),
    ]);
    renderEvents(events);
    setStatus(run.status);
    if (["completed", "failed", "cancelled"].includes(run.status)) {
      const finalEvents = await runtimeFetch(
        `/runs/${encodedRunId}/events?after_sequence=${renderedSequence}`,
      );
      renderEvents(finalEvents);
      if (run.status === "completed") {
        renderCompletedRun(run);
      } else {
        showFailure(run.error_code || run.error || `Run ${run.status}.`);
      }
      return;
    }
    await delay(POLL_INTERVAL_MS);
  }
  throw new Error(
    `Run did not reach a terminal state within ${RUN_TIMEOUT_MS / 1000}s.`,
  );
}

async function submitRequest(event) {
  event.preventDefault();
  const message = requestInput.value.trim();
  if (!message || !session) {
    return;
  }

  resetWorkspace();
  runButton.disabled = true;
  requestInput.disabled = true;
  const requestId = randomId();
  try {
    const run = await runtimeFetch("/runs", {
      method: "POST",
      body: JSON.stringify({
        thread_id: `runtime-console-${requestId}`,
        client_request_id: `runtime-console-${requestId}`,
        agent_id: session.agent_id,
        agent_version: session.agent_version,
        input: {
          user_message: message,
          requested_action: session.requested_action,
        },
      }),
    });
    await pollRun(run.run_id);
  } catch (error) {
    setStatus("failed");
    showFailure(error instanceof Error ? error.message : "Unexpected demo error.");
  } finally {
    runButton.disabled = false;
    requestInput.disabled = false;
    requestInput.focus();
  }
}

async function initialize() {
  try {
    session = await responseJson(
      await fetch("/demo/session", { headers: { Accept: "application/json" } }),
    );
    requestInput.value = session.default_message;
    runtimeVersion.textContent = `${session.agent_id}:${session.agent_version}`;
  } catch (error) {
    runButton.disabled = true;
    setStatus("failed");
    showFailure(
      error instanceof Error ? error.message : "The local demo session could not start.",
    );
  }
}

form.addEventListener("submit", submitRequest);
requestInput.addEventListener("keydown", (event) => {
  if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
    form.requestSubmit();
  }
});

initialize();
