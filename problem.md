# Problems

## H20 Is Not Complete

- H20 full-market production run was started, then paused/interrupted because H21 was requested first.
- The interruption was recovered cleanly:

```text
recovery-only attempted=5
recovered=5
failed=0
remaining_pending=0
```

- H20 must not be marked complete yet.

## Production Upload Stability

During the interrupted H20 run, upload workers reported:

```text
remote mkdir failed:
bash: warning: setlocale: LC_ALL: cannot change locale (C.UTF-8)
/bin/sh: warning: setlocale: LC_ALL: cannot change locale (C.UTF-8)
```

Current risk:

- Remote command warnings on stderr can be treated as fatal even when the actual directory creation may have succeeded.
- This can interrupt long production runs unnecessarily.

Needed fix:

- Make remote mkdir/upload verification judge success primarily by process exit code and verified remote file state.
- Treat known locale warnings as non-fatal noise when the command succeeds.

## Concurrent Runtime Error Propagation

During H20 interruption, several upload worker exceptions were printed as:

```text
Task exception was never retrieved
```

Current risk:

- Worker failures can be noisy and may not be collected into one clean runtime failure result.
- Long production runs need deterministic cancellation, shutdown, and recovery behavior.

Needed fix:

- Make `ConcurrentProductionRuntime` collect worker exceptions promptly.
- Cancel sibling workers cleanly.
- Return/raise one clear production failure.
- Ensure recovery manifests remain sufficient for retry.

## H14/H15 Remaining Nuance

H18.5 made production checkpointing safe at scope level:

```text
scope checkpoint saves only after all batches containing that scope complete
```

Remaining nuance:

- Ordered watermark logic exists at unit level, but production runtime currently uses scope-level completion rather than a full ordered watermark for all task forms.
- This is acceptable for current Naver forum scope rollout, but should be revisited before more complex task pagination or mixed datasets.

## CLI Observability Is Mostly End-of-Run

Current production CLI prints detailed JSON stats only after completion.

Impact:

- Long H20/full-market runs are hard to inspect while running.
- Queue depth, throughput, and worker progress are available in result stats, but not streamed live.

Needed improvement:

- Add periodic progress logging for long production runs.
- Include scope counts, queue depths, uploads completed, catalog completed, and pending recovery count.

## Dirty Git Worktree

The repository currently contains many modified and untracked files from completed phases.

Risk:

- It is harder to review only the current change.
- Future changes should avoid reverting unrelated work.

Needed action:

- Review and commit/shelve completed phase changes once the user is ready.
