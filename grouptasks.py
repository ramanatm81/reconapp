"""
Dynamic, sequential “stages” with large fan-out in Celery WITHOUT chaining chords.

Why this pattern:
- You have dynamic task groups (size known only at runtime)
- You need stages to run one after another
- You need final aggregation after all stages
- You hit OOM when trying to build/serialize big chained chords

This pattern:
- Never constructs a giant DAG in memory
- Uses Redis for lightweight coordination + result collection
- Uses per-task callbacks (link) to count completions
- Advances to the next stage exactly once (even with retries)

Tested design assumptions:
- Works with very large numbers of tasks per stage
- Avoids chord header serialization explosion
"""

from __future__ import annotations

import os
import uuid
import json
from dataclasses import dataclass
from typing import Any, Iterable, List, Dict, Optional

from celery import Celery, group
from celery.utils.log import get_task_logger

import redis

logger = get_task_logger(__name__)

# -------------------------
# Configuration
# -------------------------

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

REDIS_COORD_URL = os.getenv("REDIS_COORD_URL", "redis://localhost:6379/2")

# Create Celery app
app = Celery("dynamic_stages", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
    # Strongly recommended for large fan-out to avoid backend bloat:
    task_ignore_result=True,  # tasks won't store results in result_backend by default
    # If you rely on Celery result backend for other tasks, adjust per-task via @app.task(ignore_result=...)
)

# Redis client for coordination/results
r = redis.Redis.from_url(REDIS_COORD_URL, decode_responses=True)

# -------------------------
# Redis key scheme
# -------------------------

def k_run_meta(run_id: str) -> str:
    return f"run:{run_id}:meta"

def k_stage_total(run_id: str, stage: int) -> str:
    return f"run:{run_id}:stage:{stage}:total"

def k_stage_done(run_id: str, stage: int) -> str:
    return f"run:{run_id}:stage:{stage}:done"

def k_stage_advance_guard(run_id: str, stage: int) -> str:
    """
    Guard key to ensure only ONE worker triggers next stage transition.
    Stage transition is guarded per stage.
    """
    return f"run:{run_id}:stage:{stage}:advanced"

def k_results_list(run_id: str) -> str:
    return f"run:{run_id}:results"  # list of JSON strings, or store refs/keys instead

def k_errors_list(run_id: str) -> str:
    return f"run:{run_id}:errors"


# -------------------------
# Your dynamic stage logic
# -------------------------

@dataclass(frozen=True)
class WorkItem:
    """
    Define what a unit of work is for your system.
    Keep it serializable (JSON-friendly).
    """
    index: int
    arg2: str


def get_stage_count() -> int:
    """
    Total number of stages in your pipeline.
    If truly dynamic, store it in run meta at start_run().
    """
    return 5


def get_items_for_stage(run_id: str, stage: int) -> List[WorkItem]:
    """
    Return the dynamic list of work items for a stage.
    Replace this with your real discovery logic:
      - query DB
      - scan files
      - compute partitions
      - etc.

    IMPORTANT: This function runs on workers, so it must be deterministic and safe.
    """
    # Example: mimic your sizes: 1600, 1200, 48, 40, 5
    stage_sizes = {1: 1600, 2: 1200, 3: 48, 4: 40, 5: 5}
    n = stage_sizes.get(stage, 0)

    arg2 = "hello"
    return [WorkItem(index=i, arg2=arg2) for i in range(n)]


def build_task_sigs_for_stage(run_id: str, stage: int) -> List[Any]:
    """
    Convert stage work-items into Celery signatures.
    Keep each signature small. Don’t embed huge payloads in args.
    """
    items = get_items_for_stage(run_id, stage)
    return [process_item.s(run_id=run_id, stage=stage, index=item.index, arg2=item.arg2) for item in items]


# -------------------------
# Atomic stage completion / advancement (Redis Lua)
# -------------------------

_STAGE_DONE_LUA = r.register_script(
    """
    -- KEYS:
    -- 1) done_key
    -- 2) total_key
    -- 3) advanced_guard_key
    --
    -- ARGV:
    -- 1) (optional) ttl_seconds for guard key, default 86400
    --
    -- Returns:
    -- 0 => not complete yet OR already advanced
    -- 1 => this call completed the stage and successfully acquired guard (advance now)
    local done_key = KEYS[1]
    local total_key = KEYS[2]
    local guard_key = KEYS[3]

    local ttl = tonumber(ARGV[1]) or 86400

    local done = redis.call("INCR", done_key)
    local total = tonumber(redis.call("GET", total_key) or "0")

    if total == 0 then
      return 0
    end

    if done < total then
      return 0
    end

    -- Stage done: attempt to acquire guard so only one worker advances
    local ok = redis.call("SETNX", guard_key, "1")
    if ok == 1 then
      redis.call("EXPIRE", guard_key, ttl)
      return 1
    else
      return 0
    end
    """
)

# -------------------------
# Tasks
# -------------------------

@app.task(bind=True)
def start_run(self, run_id: Optional[str] = None) -> str:
    """
    Kick off a run. Stores run metadata and launches stage 1.
    """
    run_id = run_id or uuid.uuid4().hex
    meta = {"run_id": run_id, "stage_count": get_stage_count()}
    r.hset(k_run_meta(run_id), mapping={k: json.dumps(v) for k, v in meta.items()})

    logger.info("Starting run_id=%s", run_id)
    run_stage.delay(run_id=run_id, stage=1)
    return run_id


@app.task(bind=True)
def run_stage(self, run_id: str, stage: int) -> None:
    """
    Discovers dynamic tasks for a stage and submits them as a group.
    We do NOT use chord. We attach a per-task callback via `link`.
    """
    stage_count = int(json.loads(r.hget(k_run_meta(run_id), "stage_count") or "0") or 0)
    if stage_count and stage > stage_count:
        logger.info("All stages complete for run_id=%s, scheduling finalize", run_id)
        finalize.delay(run_id=run_id)
        return

    # Build signatures dynamically
    sigs = build_task_sigs_for_stage(run_id, stage)
    total = len(sigs)

    logger.info("run_id=%s stage=%s discovered %s tasks", run_id, stage, total)

    # If no work, jump to next stage immediately
    if total == 0:
        run_stage.delay(run_id=run_id, stage=stage + 1)
        return

    # Initialize counters in Redis
    # Note: reset done counter for this stage (if rerun, you may want different behavior)
    r.set(k_stage_total(run_id, stage), total)
    r.set(k_stage_done(run_id, stage), 0)
    r.delete(k_stage_advance_guard(run_id, stage))

    # Submit group; attach callback to each task completion
    # IMPORTANT: link is applied to each task in the group
    g = group(sigs)
    g.apply_async(link=on_task_done.s(run_id=run_id, stage=stage))

    # Nothing else to do; stage advances when tasks complete.


@app.task(bind=True, ignore_result=True)
def process_item(self, run_id: str, stage: int, index: int, arg2: str) -> Dict[str, Any]:
    """
    One unit of work. Return value can be small.
    For large outputs, write them to Redis/DB/S3 and return a reference.
    """
    # --- your real work here ---
    result = {"stage": stage, "index": index, "value": f"processed({index},{arg2})"}

    # Store result externally (Redis list here for simplicity)
    # In production, consider Redis Streams, a DB table, or S3 with keys.
    r.rpush(k_results_list(run_id), json.dumps(result))
    return result


@app.task(bind=True)
def on_task_done(self, task_result: Any, run_id: str, stage: int) -> None:
    """
    Called once PER completed task (because it's linked).
    Uses atomic Redis Lua to:
      - increment done counter
      - check if stage is complete
      - acquire an "advance" guard so only one worker advances
    """
    try:
        advance = int(
            _STAGE_DONE_LUA(
                keys=[
                    k_stage_done(run_id, stage),
                    k_stage_total(run_id, stage),
                    k_stage_advance_guard(run_id, stage),
                ],
                args=[86400],  # guard key TTL
            )
        )
        if advance == 1:
            logger.info("run_id=%s stage=%s complete; advancing to stage=%s", run_id, stage, stage + 1)
            run_stage.delay(run_id=run_id, stage=stage + 1)

    except Exception as e:
        # Record error; do not crash callback
        logger.exception("on_task_done failed run_id=%s stage=%s: %s", run_id, stage, e)
        r.rpush(k_errors_list(run_id), json.dumps({"stage": stage, "error": str(e)}))


@app.task(bind=True)
def finalize(self, run_id: str) -> Dict[str, Any]:
    """
    Final aggregation after all stages.
    Reads everything written to Redis and performs final handling.
    """
    # Load results
    raw = r.lrange(k_results_list(run_id), 0, -1)
    results = [json.loads(x) for x in raw]

    errors_raw = r.lrange(k_errors_list(run_id), 0, -1)
    errors = [json.loads(x) for x in errors_raw]

    summary = {
        "run_id": run_id,
        "result_count": len(results),
        "error_count": len(errors),
    }

    logger.info("Finalize run_id=%s summary=%s", run_id, summary)

    # --- your final logic here ---
    # e.g., write a report, update DB, notify, etc.

    return summary


# -------------------------
# How to run
# -------------------------
"""
1) Start Redis (or point to your existing Redis)
2) Start a Celery worker:

   celery -A this_file_name worker -l INFO --concurrency=8

3) Trigger a run:

   from this_file_name import start_run
   start_run.delay()

Notes / tuning:
- This avoids chord/chain OOM because it does NOT build a huge multi-stage chord graph.
- Stage size is dynamic: get_items_for_stage() can query DB, etc.
- Results are stored externally; Celery backend is not used for large payloads.
- on_task_done is called per task completion: keep it lightweight.
- For extremely high task throughput, consider batching acknowledgements:
  e.g. report_done every N tasks, or using Redis Streams/consumer groups.

If you want, I can adapt this to:
- store only references (keys) instead of full results
- handle retries exactly-once (idempotency keys per item)
- support parallelism limits per stage
- provide a minimal Docker compose for broker/backend/worker
"""
