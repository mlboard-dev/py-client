"""Independent single-writer process. No interpreter-exit join with training."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path
import queue
import secrets
import selectors
import socket
import time
import threading

from .ipc import extract, frame
from .store import RunStore, prepare_media_file
from .transport import sync_pass
from .validation import dumps


def serve(root, *, idle_timeout=30):
    root = Path(root).resolve()
    root.mkdir(parents=True,exist_ok=True)
    lock = (root/"writer.lock").open("a+b")
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        return
    store = RunStore(root)
    listener = socket.socket()
    listener.bind(("127.0.0.1",0))
    listener.listen(32)
    listener.setblocking(False)
    secret = secrets.token_urlsafe(32)
    endpoint = root/"collector.json"
    temporary = root/"collector.json.tmp"
    fd = os.open(temporary,os.O_CREAT|os.O_TRUNC|os.O_WRONLY,0o600)
    with os.fdopen(fd,"w") as output:
        output.write(dumps({"port":listener.getsockname()[1],"secret":secret,"pid":os.getpid()}))
    os.replace(temporary,endpoint)
    selector = selectors.DefaultSelector()
    selector.register(listener,selectors.EVENT_READ,None)
    clients = {}
    completed = queue.Queue(maxsize=1)
    media_jobs = queue.Queue(maxsize=1)
    media_completed = queue.Queue(maxsize=2)
    network_thread = None
    configs = {}
    last_work, next_sync = time.monotonic(),0.0
    failures = 0
    maintenance_at = time.monotonic()+1
    sync_passes = 0

    def disconnect(sock):
        with contextlib.suppress(Exception):
            selector.unregister(sock)
        clients.pop(sock,None)
        sock.close()

    def update(sock, client):
        if len(client["out"]) > 1024*1024:
            disconnect(sock)
        else:
            selector.modify(sock,selectors.EVENT_READ|(selectors.EVENT_WRITE if client["out"] else 0),client)

    def worker(config):
        try:
            result = sync_pass(root,config)
        except Exception:
            result = [{"run_id":rid,"error":"sync worker failed"} for rid in config["runs"]]
        completed.put(result)

    def media_worker():
        while True:
            sock,client,message,limit = media_jobs.get()
            try:
                item = prepare_media_file(root,message["media"],max_bytes=limit)
                outcome = {"item":item}
            except Exception as exc:
                outcome = {"error":f"media copy failed: {type(exc).__name__}"}
            media_completed.put((sock,client,message,outcome))

    threading.Thread(target=media_worker,daemon=True,name="mlboard-media").start()

    try:
        while True:
            media_replies = []
            for _ in range(2):
                try:
                    sock,client,message,outcome = media_completed.get_nowait()
                except queue.Empty:
                    break
                client["media_pending"] = max(0,client.get("media_pending",0)-1)
                reply={"id":message["id"],"kind":"media","ok":True}
                try:
                    if outcome.get("error"):
                        raise ValueError(outcome["error"])
                    with store.db:
                        store.publish_media(message["event"]["run_id"],outcome["item"])
                        revision,duplicate=store.append(message["event"])
                    reply.update(seq=message["event"]["seq"],revision=str(revision),duplicate=duplicate,status=store.status(message["event"]["run_id"]))
                except Exception as exc:
                    reply.update(ok=False,error=f"media persist failed: {type(exc).__name__}: {str(exc)[:128]}")
                media_replies.append((sock,client,reply))
                deferred=client.pop("finish_deferred",None) if not client.get("media_pending") else None
                if deferred:
                    finish_reply={"id":deferred["id"],"kind":"finish","ok":True}
                    try:
                        with store.db:
                            store.finish(deferred["run_id"],deferred["state"],deferred.get("dropped",0))
                        finish_reply["status"]=store.status(deferred["run_id"])
                    except Exception as exc:
                        finish_reply.update(ok=False,error=f"finish failed: {type(exc).__name__}")
                    media_replies.append((sock,client,finish_reply))
            for sock,client,reply in media_replies:
                if sock in clients:
                    client["out"].extend(frame(reply))
                    update(sock,client)
            if time.monotonic()>=maintenance_at:
                store.maintenance()
                maintenance_at=time.monotonic()+1
            try:
                result = completed.get_nowait()
            except queue.Empty:
                result = None
            if result is not None:
                sync_passes += 1
                with store.db:
                    for outcome in result:
                        store.apply_sync_result(outcome)
                failures = failures+1 if any(r.get("error") for r in result) else 0
                next_sync = time.monotonic()+min(10,0.25*(2**min(failures,6)))
                network_thread = None
            if configs and network_thread is None and time.monotonic() >= next_sync:
                config = {"runs":{rid:dict(c) for rid,c in configs.items()}}
                network_thread = threading.Thread(target=worker,args=(config,),daemon=True,name="mlboard-upload")
                network_thread.start()
            messages = []
            for key, mask in selector.select(0.02):
                if key.data is None:
                    sock,_ = listener.accept()
                    if len(clients)>=32:
                        sock.close()
                        continue
                    sock.setblocking(False)
                    sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,65536)
                    client = {"in":bytearray(),"out":bytearray(),"authed":False,"last":time.monotonic()}
                    clients[sock] = client
                    selector.register(sock,selectors.EVENT_READ,client)
                    continue
                sock,client = key.fileobj,key.data
                try:
                    if mask & selectors.EVENT_READ:
                        chunk = sock.recv(65536)
                        if not chunk:
                            disconnect(sock)
                            continue
                        client["in"].extend(chunk)
                        client["last"] = last_work = time.monotonic()
                        if len(client["in"]) > 1024*1024:
                            raise ValueError("IPC buffer exceeded")
                        for message in extract(client["in"]):
                            if not client["authed"]:
                                if message.get("kind") != "auth" or not secrets.compare_digest(message.get("secret",""),secret):
                                    raise ValueError("IPC authentication failed")
                                client["authed"] = True
                            else:
                                messages.append((sock,client,message))
                    if mask & selectors.EVENT_WRITE and client["out"]:
                        sent = sock.send(client["out"])
                        del client["out"][:sent]
                    update(sock,client)
                except (OSError,ValueError,KeyError):
                    disconnect(sock)
            replies = []
            if messages:
                try:
                    with store.db:
                        for sock,client,message in messages:
                            kind = message.get("kind")
                            reply = {"id":message.get("id"),"kind":kind,"ok":True}
                            if kind=="media":
                                try:
                                    rid=message["event"]["run_id"]
                                    if client.get("run_id")!=rid:
                                        raise ValueError("register run first")
                                    usage=store.db.execute("SELECT history_bytes,media_bytes FROM runs WHERE id=?",(rid,)).fetchone()
                                    limit=min(256*1024**2,store.max_run_bytes-usage[0]-usage[1])
                                    media_jobs.put_nowait((sock,client,message,limit))
                                    client["media_pending"]=client.get("media_pending",0)+1
                                except Exception as exc:
                                    reply.update(ok=False,error=f"media queue unavailable: {type(exc).__name__}")
                                    replies.append((sock,client,reply))
                                continue
                            if kind=="finish" and client.get("media_pending"):
                                client["finish_deferred"]=message
                                continue
                            store.db.execute("SAVEPOINT message")
                            try:
                                if kind == "register":
                                    ordinal = store.register(message)
                                    client["run_id"] = message["run_id"]
                                    reply["session_ordinal"] = str(ordinal)
                                    if message["sync"] == "background":
                                        server = message.get("server") or "http://127.0.0.1:8080"
                                        store.mark_sync(server=server,run_id=message["run_id"],force_remote_check=message.get("organization") is not None)
                                        configs[message["run_id"]] = {"server":server,"api_key":message.get("api_key"),"create_project":message.get("create_project",False),"organization":message.get("organization")}
                                elif kind == "event":
                                    if client.get("run_id") != message["event"]["run_id"]:
                                        raise ValueError("register run first")
                                    revision,duplicate = store.append(message["event"])
                                    reply.update(seq=message["event"]["seq"],revision=str(revision),duplicate=duplicate)
                                elif kind == "finish":
                                    if client.get("run_id") != message["run_id"]:
                                        raise ValueError("register run first")
                                    store.finish(message["run_id"],message["state"],message.get("dropped",0))
                                elif kind == "import_batch":
                                    manifest = message["manifest"]
                                    store.register(manifest)
                                    source,identity = message["source"],message["identity"]
                                    old = store.db.execute("SELECT identity,offset FROM import_offsets WHERE source=?",(source,)).fetchone()
                                    expected,next_offset = message["expected_offset"],message["next_offset"]
                                    if old and old["identity"] != identity:
                                        raise ValueError("TensorBoard source was replaced; choose a new import store")
                                    current = old["offset"] if old else 0
                                    if current == next_offset:
                                        reply["duplicate"] = True
                                    elif current != expected:
                                        raise ValueError("TensorBoard offset changed; restart importer")
                                    else:
                                        for event in message["events"]:
                                            store.append(event)
                                        store.db.execute("INSERT INTO import_offsets VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET offset=excluded.offset",(source,identity,next_offset))
                                    reply["offset"] = next_offset
                                elif kind == "status":
                                    pass
                                elif kind == "sync":
                                    server = message.get("server") or "http://127.0.0.1:8080"
                                    count = store.mark_sync(server=server,project=message.get("project"),force_remote_check=message.get("organization") is not None)
                                    selected_config = {"server":server,"api_key":message.get("api_key"),"create_project":message.get("create_project",False),"organization":message.get("organization"),"timeout":message.get("http_timeout",4)}
                                    selected = store.db.execute("SELECT id FROM runs WHERE pending_mode=1"+(" AND project=?" if message.get("project") else ""),(message["project"],) if message.get("project") else ()).fetchall()
                                    for row in selected:
                                        configs[row[0]] = dict(selected_config)
                                    next_sync = 0.0
                                    reply["runs"] = count
                                    reply["sync_target"] = sync_passes+1
                                else:
                                    raise ValueError("unsupported collector command")
                                store.db.execute("RELEASE message")
                                reply["status"] = store.status(message.get("run_id") or client.get("run_id"),project=message.get("project"))
                                reply["status"]["syncing"] = network_thread is not None
                                reply["status"]["sync_passes_completed"] = sync_passes
                            except Exception as exc:
                                store.db.execute("ROLLBACK TO message")
                                store.db.execute("RELEASE message")
                                reply.update(ok=False,error=f"{type(exc).__name__}: {str(exc)[:256]}")
                            replies.append((sock,client,reply))
                    # Only publish durable receipts after the outer SQLite commit.
                except Exception as exc:
                    replies = [(sock,client,{"id":m.get("id"),"kind":m.get("kind"),"ok":False,"error":f"commit failed: {type(exc).__name__}"}) for sock,client,m in messages]
                for sock,client,reply in replies:
                    if sock in clients:
                        client["out"].extend(frame(reply))
                        update(sock,client)
            for sock,client in list(clients.items()):
                if not client["authed"] and time.monotonic()-client["last"] > 2:
                    disconnect(sock)
            if not clients and time.monotonic()-last_work > idle_timeout:
                # Pending data remains durable. The CLI can continue after exit.
                break
    finally:
        for sock in list(clients):
            disconnect(sock)
        selector.close()
        listener.close()
        with contextlib.suppress(OSError,ValueError):
            if json.loads(endpoint.read_text()).get("pid") == os.getpid():
                endpoint.unlink()
        store.close()
        lock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root",required=True)
    parser.add_argument("--idle-timeout",type=float,default=30)
    args = parser.parse_args()
    serve(args.root,idle_timeout=args.idle_timeout)


if __name__ == "__main__":
    main()
