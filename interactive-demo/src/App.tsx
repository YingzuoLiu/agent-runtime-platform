import { useEffect, useMemo, useState } from "react";

type ComponentName = "Router" | "Memory" | "Planner" | "Executor" | "Validator" | "Reducer" | "State";

type RuntimeState = {
  stage: string;
  destination: string | null;
  days: number | null;
  budget: number | null;
  preferences: Record<string, string | boolean>;
  plan: null | { flight: string; hotel: string; itinerary: string; estimatedCost: number };
  validation: "pending" | "failed" | "passed";
  blockers: string[];
  retryCount: number;
};

type Step = {
  component: ComponentName;
  title: string;
  subtitle: string;
  narration: string;
  event: string;
  state: RuntimeState;
};

const initialState: RuntimeState = {
  stage: "initialized",
  destination: null,
  days: null,
  budget: null,
  preferences: {},
  plan: null,
  validation: "pending",
  blockers: [],
  retryCount: 0,
};

const steps: Step[] = [
  {
    component: "Router",
    title: "Route the request",
    subtitle: "Intent · create_plan",
    narration: "The Router classifies the request and chooses the planning path. It does not create the itinerary itself.",
    event: "intent.detected",
    state: initialState,
  },
  {
    component: "Memory",
    title: "Load thread memory",
    subtitle: "Thread · interview-demo-001",
    narration: "Memory retrieves the durable checkpoint. This is task state, not a transcript stuffed back into the prompt.",
    event: "checkpoint.loaded",
    state: { ...initialState, stage: "context_loaded" },
  },
  {
    component: "Planner",
    title: "Create a structured patch",
    subtitle: "Tokyo · 5 days · SGD 9,000",
    narration: "The Planner converts user intent into an explicit StatePatch and marks downstream itinerary fields for replanning.",
    event: "patch.proposed",
    state: {
      ...initialState,
      stage: "planning",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced" },
    },
  },
  {
    component: "Executor",
    title: "Call travel tools",
    subtitle: "Flights · hotels · POIs",
    narration: "The Executor runs deterministic tool adapters. In this repository they are simulated so the demo remains offline-first.",
    event: "tools.completed",
    state: {
      ...initialState,
      stage: "executing",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced" },
      plan: { flight: "red-eye", hotel: "standard", itinerary: "balanced", estimatedCost: 9100 },
    },
  },
  {
    component: "Validator",
    title: "Reject an invalid plan",
    subtitle: "Budget exceeded · SGD 100",
    narration: "The Validator checks hard constraints outside the model. A fluent answer cannot bypass budget policy.",
    event: "validation.failed",
    state: {
      ...initialState,
      stage: "validation_failed",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced" },
      plan: { flight: "red-eye", hotel: "standard", itinerary: "balanced", estimatedCost: 9100 },
      validation: "failed",
      blockers: ["Estimated cost exceeds budget by SGD 100"],
    },
  },
  {
    component: "Reducer",
    title: "Reduce failure into state",
    subtitle: "Retry · 1 of 2",
    narration: "The Reducer is the only writer. It applies the validation patch atomically and makes the transition replayable.",
    event: "state.reduced",
    state: {
      ...initialState,
      stage: "replanning",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced" },
      plan: { flight: "red-eye", hotel: "standard", itinerary: "balanced", estimatedCost: 9100 },
      validation: "failed",
      blockers: ["Estimated cost exceeds budget by SGD 100"],
      retryCount: 1,
    },
  },
  {
    component: "Planner",
    title: "Partially replan",
    subtitle: "Change hotel only",
    narration: "The Planner preserves valid work and changes only the affected sub-plan. This limits cost, latency, and behavioral drift.",
    event: "replan.scoped",
    state: {
      ...initialState,
      stage: "replanning",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced", optimization: "budget" },
      plan: { flight: "red-eye", hotel: "budget", itinerary: "balanced", estimatedCost: 8600 },
      retryCount: 1,
    },
  },
  {
    component: "Executor",
    title: "Execute the scoped change",
    subtitle: "Hotel search refreshed",
    narration: "The Executor re-runs only the hotel tool. Existing flight and itinerary outputs remain stable.",
    event: "tool.hotel.completed",
    state: {
      ...initialState,
      stage: "executing",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced", optimization: "budget" },
      plan: { flight: "red-eye", hotel: "budget", itinerary: "balanced", estimatedCost: 8600 },
      retryCount: 1,
    },
  },
  {
    component: "Validator",
    title: "Validate the new plan",
    subtitle: "All hard constraints pass",
    narration: "The same deterministic rules run again. The plan is now within budget and can advance.",
    event: "validation.passed",
    state: {
      ...initialState,
      stage: "validated",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced", optimization: "budget" },
      plan: { flight: "red-eye", hotel: "budget", itinerary: "balanced", estimatedCost: 8600 },
      validation: "passed",
      retryCount: 1,
    },
  },
  {
    component: "Reducer",
    title: "Commit the checkpoint",
    subtitle: "Version · v4",
    narration: "The Reducer commits the accepted patch and the runtime persists a checkpoint for continuation, recovery, and audit.",
    event: "checkpoint.committed",
    state: {
      ...initialState,
      stage: "completed",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced", optimization: "budget" },
      plan: { flight: "red-eye", hotel: "budget", itinerary: "balanced", estimatedCost: 8600 },
      validation: "passed",
      retryCount: 1,
    },
  },
  {
    component: "State",
    title: "Return a grounded response",
    subtitle: "Completed · trace available",
    narration: "The final response is rendered from validated structured state. The complete event trail remains inspectable.",
    event: "run.completed",
    state: {
      ...initialState,
      stage: "completed",
      destination: "Tokyo",
      days: 5,
      budget: 9000,
      preferences: { travelStyle: "balanced", optimization: "budget" },
      plan: { flight: "red-eye", hotel: "budget", itinerary: "balanced", estimatedCost: 8600 },
      validation: "passed",
      retryCount: 1,
    },
  },
];

const components: ComponentName[] = ["Router", "Memory", "Planner", "Executor", "Validator", "Reducer", "State"];

function App() {
  const [index, setIndex] = useState(0);
  const [playing, setPlaying] = useState(false);
  const step = steps[index];
  const progress = ((index + 1) / steps.length) * 100;

  useEffect(() => {
    if (!playing) return;
    if (index === steps.length - 1) {
      setPlaying(false);
      return;
    }
    const timer = window.setTimeout(() => setIndex((value) => value + 1), 1500);
    return () => window.clearTimeout(timer);
  }, [index, playing]);

  const trace = useMemo(() => steps.slice(0, index + 1), [index]);

  const reset = () => {
    setPlaying(false);
    setIndex(0);
  };

  return (
    <main>
      <header className="hero">
        <div>
          <p className="eyebrow">INTERACTIVE SYSTEM DESIGN WALKTHROUGH</p>
          <h1>Travel Agent Runtime <span>Explorer</span></h1>
          <p className="lede">See how planning, durable state, validation, and controlled execution cooperate—one transition at a time.</p>
        </div>
        <div className="status"><i /> offline-first demo</div>
      </header>

      <section className="scenario">
        <div>
          <small>USER REQUEST</small>
          <p>“Plan a 5-day Tokyo trip under SGD 9,000.”</p>
        </div>
        <div className="controls">
          <button className="secondary" onClick={reset}>Reset</button>
          <button className="primary" onClick={() => setPlaying((value) => !value)}>{playing ? "Pause" : "Auto play"}</button>
        </div>
      </section>

      <nav className="pipeline" aria-label="Runtime components">
        {components.map((name, componentIndex) => {
          const activeIndex = components.indexOf(step.component);
          return (
            <div className={`component ${name === step.component ? "active" : ""} ${componentIndex < activeIndex ? "visited" : ""}`} key={name}>
              <b>{componentIndex + 1}</b><span>{name}</span>
            </div>
          );
        })}
      </nav>

      <div className="progress"><span style={{ width: `${progress}%` }} /></div>

      <section className="workspace">
        <article className="stage-card">
          <div className="step-meta"><span>STEP {String(index + 1).padStart(2, "0")} / {steps.length}</span><code>{step.event}</code></div>
          <div className="component-label">{step.component}</div>
          <h2>{step.title}</h2>
          <h3>{step.subtitle}</h3>
          <p>{step.narration}</p>
          <div className="callout">
            <b>Interview signal</b>
            <span>{interviewSignal(step.component)}</span>
          </div>
          <div className="step-controls">
            <button disabled={index === 0} onClick={() => { setPlaying(false); setIndex((value) => value - 1); }}>← Previous</button>
            <button disabled={index === steps.length - 1} onClick={() => { setPlaying(false); setIndex((value) => value + 1); }}>Next transition →</button>
          </div>
        </article>

        <aside className="state-panel">
          <div className="panel-title"><span>AGENT STATE</span><i className={step.state.validation} /></div>
          <pre>{JSON.stringify(step.state, null, 2)}</pre>
        </aside>
      </section>

      <section className="trace-panel">
        <div className="panel-title"><span>EXECUTION TRACE</span><small>{trace.length} events</small></div>
        <div className="trace-list">
          {trace.map((item, traceIndex) => (
            <button key={`${item.event}-${traceIndex}`} onClick={() => { setPlaying(false); setIndex(traceIndex); }} className={traceIndex === index ? "selected" : ""}>
              <span>{String(traceIndex + 1).padStart(2, "0")}</span><b>{item.event}</b><small>{item.component}</small>
            </button>
          ))}
        </div>
      </section>

      <footer>
        <span>Core idea: models propose; the runtime controls.</span>
        <span>Router → Memory → Planner → Executor → Validator → Reducer → State</span>
      </footer>
    </main>
  );
}

function interviewSignal(component: ComponentName) {
  const signals: Record<ComponentName, string> = {
    Router: "Separate routing policy from business logic; make fallback behavior explicit.",
    Memory: "Distinguish durable task state, episodic history, and retrieval context.",
    Planner: "Prefer typed patches and scoped replanning over rewriting the whole plan.",
    Executor: "Add timeouts, retries, idempotency keys, and side-effect boundaries around tools.",
    Validator: "Enforce hard constraints deterministically; use models only for soft judgments.",
    Reducer: "Centralize writes so transitions can be replayed, audited, and recovered.",
    State: "Persist checkpoints and trace events outside prompts; version their schemas.",
  };
  return signals[component];
}

export default App;
