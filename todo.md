# Todo

## Immediate Next Step

Before resuming H20, fix the two production issues exposed by the interrupted full-market run:

1. Upload stderr handling:
   - tolerate known locale warnings when the remote command exit code and verification are successful;
   - keep real SSH/rsync/mkdir failures fatal.

2. Concurrent runtime worker failure handling:
   - collect exceptions from upload/catalog/crawl/write workers;
   - cancel sibling workers cleanly;
   - avoid `Task exception was never retrieved`;
   - leave recovery manifests in a retryable state.

Required validation after these fixes:

```text
focused upload/runtime tests
pytest -q -p no:rerunfailures tests/unit
recovery-only production check
```

## Resume H20

Run the full-market production crawl from the same authoritative snapshot:

```bash
python -m crawl_framework.cli.main crawl \
  --site naver_finance \
  --dataset forum_post \
  --instruments-file config/universes/naver_finance_kr_rollout_universe.txt \
  --max-pages 1 \
  --crawl-workers 8 \
  --writer-workers 1 \
  --upload-workers 4 \
  --catalog-workers 4 \
  --target-file-size-mb 128 \
  --json
```

Acceptance criteria:

```text
success=true
remaining_pending=0
errors=0
checkpoints complete for full snapshot
uploads_started == uploads_completed
catalog_jobs_started == catalog_jobs_completed
bounded queue depths are healthy
crawl/upload/catalog overlap is visible in production stats
```

After H20:

```text
run recovery-only
run pytest -q -p no:rerunfailures tests/unit
update project_status.md
```

## Phase H Finalization

- H21 is already complete, but after H20 succeeds, re-run the H21 focused tests once more as a final multi-site regression check.
- Update `project_status.md` to mark H20 complete.
- Confirm whether Phase H as a whole is complete against the acceptance criteria in `Naver_Universal_Parallel_Production_Pipeline_Plan.md`.

## Useful Follow-Up Work

- Add live periodic production progress logs instead of only end-of-run JSON.
- Add a compact production summary output mode so large interval arrays do not overwhelm CLI output.
- Add checkpoint-completeness verification command for a given rollout snapshot and limit.
- Review and commit completed phase changes in logical commits.
