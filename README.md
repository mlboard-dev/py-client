# MLBoard Python client

A runnable, dependency-free Python 3.11+ SDK, independent collector, explicit HTTP synchronizer, and authenticated local viewer. The collector stores real events in a local SQLite RunStore; the central Go service has its own separate database. No demo data is inserted automatically.

From the workspace root, install into a project virtual environment:

```sh
export MLBOARD_ROOT="$PWD"
. ./scripts/env.sh
python3 -m venv outputs/client-venv
outputs/client-venv/bin/python -m pip install -e './src/py-client[serve]'
```

`serve` currently adds no dependencies: its HTTP adapter, SVG plots and frontend resources ship in the wheel. Importing `mlboard` does not import the viewer or TensorBoard. For source-tree use, set `PYTHONPATH=src/py-client`.

```python
import mlboard

with mlboard.init(
    project="vision",
    run_path="run-series1/resnet18/seed-1",
    config={"lr": 0.01, "seed": 1},
    spool_dir="./outputs/mlboard-runs",
    sync="off",
    retention="record",
) as run:
    for step in range(100):
        # Values must already be small built-in CPU ints/floats.
        receipt = run.log({"train/loss": 1 / (step + 1)}, step=step, epoch=step / 20)
        if not receipt.accepted:
            print(receipt.reason)
    print(run.status())
```

`mlboard.log()`, `mlboard.status()` and `mlboard.finish()` operate on the most recently initialized run in the current Python context. Run handles are preferred when working with several experiments. The context manager calls `finish`; it preserves the original training exception, including when strict logging fails during exception cleanup.

`init()` generates IDs and starts a daemon sender; it does not wait for disk, collector startup or a remote service. `log()` validates bounded built-in values and attempts a nonblocking in-memory enqueue. It never opens SQLite, touches a media file, performs network I/O, or invokes tensor `.item()`, `.cpu()` or `.numpy()`. Python scheduling/GC prevents a hard real-time latency guarantee. By default the queue holds at most 512 events / 8 MiB, with 128 fields / 64 KiB per event. Full or contended queues drop the new event and return a negative receipt. A bounded IPC sender starts or discovers one detached collector per store. No multiprocessing queue or interpreter-exit child join is used.

Accepted means **memory queued**. `run.finish(timeout=2, wait_for="local")` waits, within one finite budget, for committed SQLite receipts and the terminal run state. `wait_for="remote"` additionally waits for event ACKs and a durable manifest checkpoint confirming registration, sessions/producers and terminal remote state, including for zero-event runs. A failed terminal PATCH remains incomplete after events are ACKed. `wait_for="none"` just requests completion. A timed-out call leaves the sender trying to drain queued data in the background. Process termination can still lose events that have not reached local durability. `mode="strict"` raises for validation and unsuccessful explicit finish; normal safe mode reports logging errors in receipts/status. Invalid initialization options raise immediately.

Calling `finish(wait_for="remote")` again after a local finish refreshes its durable checkpoint within the new call's finite budget. It can restart the collector and restore this explicit run's original credentials without changing the terminal state or event identities.

The collector is the only source writer, holding a process lock. SQLite uses WAL + FULL synchronization and bounded write batches; media is stream-copied/fsynced by one separate worker. Network synchronization runs on another worker and never holds a source read transaction across HTTP. The collector exits after 30 idle seconds with no connected clients; unacknowledged local records remain available to `sync`.

Run/session/producer IDs and sequence numbers survive replay. Resuming with `run_id=existing_id` creates a new persisted session ordinal. An existing session/producer cannot be restarted with a fresh producer token: use a new session or producer incarnation. DDP callers may supply shared `run_id`, `session_id`, `session_ordinal`, and `primary_producer`, with a distinct `producer_id` per rank. There are no implicit barriers or reductions. Run paths are organization metadata and never become storage filenames.

## Structured values and files

Helpers operate only on explicitly supplied CPU data and have bounded synchronous validation/derivation costs. They do not sample arrays or access GPUs. Histogram/matrix/threshold-curve helpers require a step and emit the versioned native payload schemas.

Threshold-curve helpers derive rates from supplied TP/FP/FN/TN counts and accept only `averaging="binary"`, `"micro"` or `"per_class"`. Macro and weighted averages require separate per-class rate calculations; labeling aggregate counts with those modes is rejected.

```python
run.log_histogram("weights/layer1", bin_edges=[-2, -1, 0, 1, 2], counts=[1, 4, 4, 1], step=9)
run.log_confusion_matrix("val/confusion", [[43, 7], [4, 46]], labels=["cat", "dog"], step=9)
run.log_roc("val/roc", thresholds=[float("-inf"), .3, .7, .9, float("inf")],
            tp=[10, 9, 6, 2, 0], fp=[10, 4, 1, 0, 0],
            fn=[0, 1, 4, 8, 10], tn=[0, 6, 9, 10, 10], step=9)
# log_pr() uses the same threshold/count schema; rates and trapezoid ROC AUC are derived explicitly.
run.log_text("notes/training", "Learning rate reduced after epoch 2", step=9)
run.log_table("val/examples", ["label", "prediction"], [["cat", "cat"]], step=9)
run.log_media("files/report", "./outputs/report.txt", kind="file", caption="Report", step=9)
run.log_message("Checkpoint completed", level="info", step=9)
```

`log_media()` only queues a path reference. The caller must retain the file unchanged until local completion. The independent media worker checks regular-file status, a 256 MiB limit, size/mtime stability, streams to an internal UUID filename, hashes, fsyncs and atomically publishes it. Its bounded queue can reject excess concurrent media; status and finish expose the rejection. The same offline artifact UUID is uploaded before its referencing event; retries never rewrite that event body.

## Local viewer

```sh
outputs/client-venv/bin/mlboard serve --logdir ./outputs/mlboard-runs --port 8081 --open
```

The default listener is `127.0.0.1:8081`; `--port 0` chooses an available port. The printed URL has a random session bootstrap credential in its fragment, immediately removed by the page after exchange for an HttpOnly/SameSite cookie. Host and Origin checks prevent ordinary cross-origin and DNS rebinding reads. Non-loopback binding requires an explicit `--token`; SSH forwarding is usually sufficient for remote training machines. Secrets are not written to access logs. The viewer writes no source event, ACK or schema data and does not start the central server or trigger synchronization.

The bundled responsive viewer includes project/run path groups, run selection and comparison, collapsible metric groups, step/epoch/wall-time/elapsed/sample axes, smoothing, histograms, confusion matrices, ROC and precision–recall curves, native structured records, config/summary, files, logs, a JSON query inspector, and storage/synchronization diagnostics. No active HTML is rendered. Axis, smoothing and group expansion preferences are stored in the browser. Changes refresh through one visibility-aware timer; collapsed groups issue no chart queries. UI queries fit 20 series across at most 10 selected runs; the inspector allows explicit narrower selection.

The public local `/api/v1` read API supports capabilities, projects/categories/runs, series, original events, sessions, logs, records, artifacts/content, local status, and `POST /projects/{id}/query`. Query sources currently support scalar frames; native records have their separate read endpoint and visual renderers. Query transforms are `raw` (`none` is an alias) and `downsample` with `min_max` version 1. `consistency`, `reuse`, `viewport`, `read.at_revision`, unknown fields and unsupported policies return explicit errors. Local capabilities advertise this precise profile; central account/ACL/dashboard editing APIs are not emulated. Summary keys are canonical JSON `[name, context]` tuples; values also include `name` and complete `context`.

The collector maintains indexed typed scalar/record projections transactionally with events. The viewer opens the source with `mode=ro` and `query_only`, uses short reads and a 2-second cancellation budget, and never holds a source transaction over browser pagination. Scalar queries select primary or explicit producers, all/latest/explicit sessions, then merge repeated steps by session ordinal and sequence **before** filtering the selected x range. Integer step/sample coordinates and bounds retain int63 precision; wall-time comparison parses RFC3339 offsets and nanoseconds. Missing x coordinates are reported; nonfinite values become explicit gaps. Min/max downsampling uses actual X-coordinate buckets and preserves first/min/max/last points plus gap boundaries and adjacent finite points; insufficient budgets return errors. Initial limits are 10 runs, 20 series, 2,000 points per series, 20,000 points total, 250,000 scanned points and a 2 MiB result. Exceeding a limit returns a diagnostic error. Up to eight paged snapshots hold bounded results for 120 seconds; cursors bind query semantics and invalidate on source replacement or selected-run cache deletion. Single-page refreshes do not occupy or evict paged snapshots. This first release has no multiresolution index for million-point queries; select a smaller retained data set or use the center for larger workloads.

Category selectors distinguish omission from an empty selection: omit both `path_prefixes` and `include_ungrouped` to include every category. An explicit empty prefix list selects no groups; `include_ungrouped=true` additionally selects only root-level runs. A category prefix matches its descendants, excluding a same-named root run. The run-directory endpoint uses `path_prefix=` for root-level runs. Record/log cursors include each row's identity so a single event can span several pages without dropping records.

## Uploads and retention

Local recording is the default. Setting `server=` alone does not enable uploads.

```sh
# Supply a permitted bearer key through the environment, not through source code.
export MLBOARD_SERVER=http://127.0.0.1:8080
# Set MLBOARD_API_KEY using your deployment's secret mechanism.
outputs/client-venv/bin/mlboard sync ./outputs/mlboard-runs --timeout 30
outputs/client-venv/bin/mlboard status ./outputs/mlboard-runs
```

For continuous upload, pass `sync="background"` to `init`, plus `server` and an explicit `api_key` or `MLBOARD_API_KEY`. The default center URL is `http://127.0.0.1:8080`. The collector resolves a permitted project ID or slug through `/projects`. It creates a project only with `create_project=True` / `--create-project` and a global key with the required `projects:write` scope; project-scoped keys require an existing project. API redirects are refused so authorization headers cannot follow a redirect to another origin. Keys stay in process memory and are excluded from persisted events and diagnostics. After a collector restart, a new registration configures credentials only for its explicit run; retained runs need their own registration or explicit manual sync. The CLI's `--project` checkpoint covers only the selected local project.

`sync=off|background` and `retention=record|cache` are independent. `record` retains event bodies and media after remote ACK; the upload reference is cleared. Explicit `cache` reclaims only fully ACKed events and unreferenced uploaded media, and declares incomplete coverage. `off+cache` is rejected. Manual sync establishes references for still-retained, unacknowledged history. A run binds to one server/project mapping; silently reusing its identity for another remote target is rejected.

Lost responses are handled by sending the exact stored event identity/body again. Identical duplicates succeed, and a different payload with an existing identity is a conflict; existing history is never overwritten. Remote metadata/sessions/producers are registered before events. Uploads use bounded 512-event / <1 MiB batches, with one artifact per pass after the scalar batch. Full remote validation failures are retained for explicit repair, rather than discarded as successful ACKs.

RunStore defaults to a 2 GiB logical per-run and 10 GiB physical store budget, plus a 64 MiB WAL checkpoint budget. Durable incremental counters make append accounting independent of history length. Physical/free-space probes and checkpoints are amortized between batches. Quota or disk failures reject new writes while retaining previous data. A single bounded copy or transaction can temporarily exceed an accounting target; filesystem and process failures can lose uncommitted events. Network filesystems, Windows collectors, cross-host shared spools, huge media stress and million-point interactive performance are not claimed as validated. Do not copy a live SQLite main file as a backup; use SQLite's supported backup procedure and an accompanying immutable media manifest.

## TensorBoard compatibility (optional)

```sh
outputs/client-venv/bin/python -m pip install -e './src/py-client[tensorboard]'
outputs/client-venv/bin/mlboard import ./outputs/tb-events --logdir ./outputs/imported --project legacy
outputs/client-venv/bin/mlboard watch ./outputs/tb-events --logdir ./outputs/imported --interval 2
```

The optional tested version is TensorBoard **2.20.0** (Python 3.14.2, protobuf 7.36.2). The importer reads original TFRecords incrementally, validates both CRC32C checksums, ignores incomplete tails until completed, and commits complete-frame offsets atomically with imported events through the sole collector writer. It imports `simple_value` and scalar numeric TensorProto summaries. Images/audio/graphs/histograms/non-scalar tensors are counted as unsupported; they are not silently converted into scalar data. Source replacement/truncation is rejected with an explicit recovery instruction. No TensorFlow dependency or global package installation is required.

## Demo and checks

```sh
PYTHONPATH=src/py-client python3 src/py-client/examples/demo.py --logdir outputs/demo
PYTHONPATH=src/py-client python3 -m mlboard serve --logdir outputs/demo
PYTHONPATH=src/py-client python3 -m unittest discover -s src/py-client/tests -v
node --test src/py-client/tests/test_query_scheduler.mjs
```

The explicit demo generates four deterministic synthetic experiments under `run-series1` and `run-series2` through the actual SDK and collector, including all native helpers/files/logs. Add `--sync` to send them to a configured center, optionally `--create-project` with a suitably scoped key. It exits unsuccessfully if its chosen finish checkpoint is incomplete.

Tests cover bounded training behavior without disk/network/tensor conversion, offline local durability, queue drops, NaN/Inf, finite finish, preserved training exceptions, actual collector kill/reconnect, session resume, immutable media, duplicate/conflicting identities, ACK loss and exact replay, empty-run/terminal-state checkpoints, revision conflicts, credential isolation across two services/runs, record/cache retention, quota rejection, append work after 5,000 history events, int63/time precision, bounded query snapshots, query-bound cursors, extrema/gaps, context-aware summaries, same-event record/log pagination, viewer authorization/Host/Origin checks, TFRecord CRC/partial tails, and optional real TensorBoard protobuf import. Test files, environments, caches and runtime stores belong under workspace `outputs/`.
