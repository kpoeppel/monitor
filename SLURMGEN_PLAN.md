# Slurm-Gen Integration Plan

Goal: Reintroduce optional `slurm_gen`-based script generation so SLURM jobs can be configured from a structured config, while still supporting plain `command` submissions. This enables overrides by mutating the slurm_gen config (including the final command).

## Proposed Design

- **New client**: `SlurmGenClient` implements `JobClientProtocol`.
  - Inputs: `slurm_gen` config payload + optional overrides.
  - Output: generated sbatch script path + submit via existing slurm client.
  - Keeps parity with `LocalCommandClient` signatures (uses `command` for legacy path).

- **Config model changes**
  - Add `slurm` block to `JobRegistration` for slurm-gen config.
  - Keep `command: list[str]` for direct submissions (existing behavior).
  - Allow `command` overrides in `slurm` block (e.g., `slurm.command` or `slurm.run.command`) to change the final executable arguments.

- **Overrides**
  - Extend restart/duplicate adjustments to update `slurm` config block.
  - Allow incremental updates (e.g., `slurm.overrides: {...}`) that merge into base config.
  - Keep `extra_args` behavior for command-only jobs; for slurm-gen jobs, `extra_args` should map into the slurm-gen command (final command array).

- **Script generation flow**
  - On submit, if `slurm` block exists:
    1) Merge `slurm` config + overrides.
    2) Generate sbatch script via slurm_gen (stored under a configured output dir).
    3) Submit script path using slurm client.
  - If no `slurm` block: submit `command` as today.

- **Top-level YAML support**
  - Extend `MonitorAppConfig` so jobs can include `slurm` config.
  - Add `client.class_name: SlurmGenClient` and a `client.slurm_gen` config for defaults/output dir.

- **Docs + Examples**
  - Add a YAML example with `slurm` block and overrides.
  - Update README to show slurm-gen usage and how overrides affect the final command.

## Implementation Steps

1. **Add config structures**
   - `SlurmGenClientConfig`, `SlurmGenJobConfig` (for job-level config block).
   - Extend `JobRegistration` and persistence to store `slurm` block.

2. **Implement SlurmGenClient**
   - Accept base config, job overrides, output dir, and optional slurm client.
   - Generate sbatch via slurm_gen, submit with log paths.

3. **Apply adjustments**
   - Add `slurm` and `slurm_overrides` to Restart/Duplicate adjustments.
   - Merge logic in `Executor._apply_adjustments`.

4. **Wire into YAML app loader**
   - Parse `slurm` block and pass to `JobRegistration`.
   - Add `client.class_name` support for `SlurmGenClient`.

5. **Docs + tests**
   - Unit tests for config merge and submission path selection.
   - Example YAML and README updates.

## Open Questions

- Exact slurm_gen schema to target (which config keys map to “final command”?).
- Where to store generated scripts by default (per state dir, cwd, or explicit output dir)? Should be configurable.
- Should `extra_args` map into slurm-gen command for consistency, or stay only for direct command submissions?
