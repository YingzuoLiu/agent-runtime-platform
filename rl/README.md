# Reinforcement-learning archive

This directory is a retained historical research artifact from an earlier exploration of intent
policy training. It is not part of the supported Agent Runtime Platform execution path.

In particular:

- `trl` and `datasets` are optional experiment dependencies and are intentionally absent from the
  repository's runtime and development requirements;
- CI does not run the training scripts or make a reproducibility claim for them;
- the scripts do not define, deploy, or improve the runtime's current Planner contract; and
- changes to Runtime Core are not evaluated through this directory.

The maintained deterministic behavioral evaluation harness is [`../eval/`](../eval/). Runtime
contracts and their executable evidence start at the [documentation index](../docs/README.md).
Consequently, this directory is not a supported or reproducible product path.
