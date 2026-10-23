"""Explicit disk/network entry points; failures have a nonzero CLI exit status."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import sys
import time

from .ipc import command
from .store import readonly
from .validation import dumps, normalize_server, text


def sync(root, *, server="http://127.0.0.1:8080", api_key=None, project=None, create_project=False, organization=None, timeout=30):
    if type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<=0:
        raise ValueError("timeout must be finite and positive")
    organization = organization if organization is not None else os.environ.get("MLBOARD_ORGANIZATION")
    if organization is not None and not text(organization,"organization",256):
        raise ValueError("organization must be a nonempty ID or slug")
    deadline = time.monotonic()+timeout
    reply = command(root,{"kind":"sync","server":normalize_server(server),"api_key":api_key or os.environ.get("MLBOARD_API_KEY"),"project":project,"create_project":create_project,"organization":organization},timeout=min(timeout,10))
    target = reply.get("sync_target",1)
    status = reply["status"]
    while time.monotonic()<deadline:
        reply = command(root,{"kind":"status","project":project},timeout=min(5,max(.001,deadline-time.monotonic())))
        status = reply["status"]
        if status.get("sync_passes_completed",0)>=target and status.get("remote_complete",False):
            return status
        time.sleep(min(.15,max(0,deadline-time.monotonic())))
    reason = '; '.join(status.get("sync_errors",[])) or "remote checkpoint timeout; local history retained"
    raise RuntimeError(reason)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="mlboard",description="MLBoard local experiment recorder")
    parser.add_argument("--version",action="version",version="mlboard 0.1.0")
    sub = parser.add_subparsers(dest="command",required=True)
    p = sub.add_parser("sync",help="upload retained local history without changing event identities")
    p.add_argument("logdir")
    p.add_argument("--server",default=os.environ.get("MLBOARD_SERVER","http://127.0.0.1:8080"))
    p.add_argument("--project",help="select local project name/ID")
    p.add_argument("--create-project",action="store_true",help="explicitly allow project creation (requires global projects:write)")
    p.add_argument("--organization",help="organization ID/slug for project creation (default MLBOARD_ORGANIZATION)")
    p.add_argument("--timeout",type=float,default=30)
    p = sub.add_parser("serve",help="start authenticated local read-only viewer")
    p.add_argument("--logdir",required=True)
    p.add_argument("--host",default="127.0.0.1")
    p.add_argument("--port",type=int,default=8081)
    p.add_argument("--token",help="explicit local authentication token, required for non-loopback bind")
    p.add_argument("--open",action="store_true")
    p = sub.add_parser("status",help="inspect retained history, pending uploads and errors")
    p.add_argument("logdir")
    for name in ("import","watch"):
        p = sub.add_parser(name,help="incrementally import TensorBoard TFRecords"+(" continuously" if name=="watch" else ""))
        p.add_argument("source")
        p.add_argument("--logdir",required=True)
        p.add_argument("--project",default="tensorboard")
        p.add_argument("--format",choices=["tensorboard"],default="tensorboard")
        if name=="watch":
            p.add_argument("--interval",type=float,default=2)
    args = parser.parse_args(argv)
    try:
        if args.command=="sync":
            if args.timeout<=0:
                raise ValueError("timeout must be positive")
            result = sync(args.logdir,server=args.server,project=args.project,create_project=args.create_project,organization=args.organization,timeout=args.timeout)
            print(dumps(result))
        elif args.command=="serve":
            from .viewer import serve
            serve(args.logdir,host=args.host,port=args.port,token=args.token,open_browser=args.open)
        elif args.command=="status":
            with readonly(args.logdir) as db:
                counts = dict(db.execute("SELECT history_events,pending_events,acked_events,history_bytes,media_bytes FROM store_stats WHERE singleton=1").fetchone())
                result = {**counts,"runs":[dict(r) for r in db.execute("SELECT id,project,run_path,state,retention,last_error,dropped,manifest_revision,remote_manifest_revision FROM runs ORDER BY project,run_path LIMIT 1000")]}
            print(dumps(result))
        elif args.command in ("import","watch"):
            from .tensorboard import import_directory
            while True:
                print(dumps(import_directory(args.source,args.logdir,project=args.project)),flush=True)
                if args.command=="import":
                    break
                if not 0.1<=args.interval<=3600:
                    raise ValueError("watch interval must be 0.1..3600 seconds")
                time.sleep(args.interval)
        return 0
    except KeyboardInterrupt:
        return 130
    except (OSError,ValueError,RuntimeError,ImportError) as exc:
        print(f"mlboard: {exc}",file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
