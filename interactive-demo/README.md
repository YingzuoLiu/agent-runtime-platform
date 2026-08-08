# Interactive Runtime Explorer

An interview-friendly, step-by-step visualization of how the travel Agent Runtime coordinates:

`Router → Memory → Planner → Executor → Validator → Reducer → State`

## Run locally

```bash
cd interactive-demo
npm install
npm run dev
```

Open the local URL printed by Vite. Use **Next transition** for a guided walkthrough or **Auto play** for a hands-free demo.

## Build

```bash
npm run build
npm run preview
```

## Scope

This frontend visualizes runtime concepts and state transitions. It deliberately uses a deterministic in-browser scenario; it does not call flight, hotel, payment, or booking APIs. The Python runtime in the repository remains the executable reference implementation.

## Suggested interview flow

1. Start with the Router and explain why routing policy is separate from planning.
2. Contrast durable task state with prompt history when Memory loads the checkpoint.
3. Show the typed patch produced by the Planner.
4. Pause on the failed validation and explain deterministic guardrails.
5. Show partial replanning and why it preserves valid work.
6. End on the committed checkpoint and discuss replay, recovery, and observability.
