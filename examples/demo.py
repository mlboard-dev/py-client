"""Generate explicit deterministic synthetic experiment data through the real SDK.

PYTHONPATH=src/py-client python3 src/py-client/examples/demo.py --logdir outputs/demo
Optional remote upload: --sync --server http://127.0.0.1:8080 --project vision
Set MLBOARD_API_KEY in the environment; never put secrets in this script.
"""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
import math
from pathlib import Path
import random
import sys
import time
import mlboard


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logdir",default="./outputs/demo")
    parser.add_argument("--project",default="vision")
    parser.add_argument("--server",default="http://127.0.0.1:8080")
    parser.add_argument("--sync",action="store_true")
    parser.add_argument("--create-project",action="store_true")
    parser.add_argument("--steps",type=int,default=80)
    parser.add_argument("--pause",type=float,default=0)
    args = parser.parse_args()
    spool = Path(args.logdir).resolve()
    spool.mkdir(parents=True,exist_ok=True)
    note = spool/"demo-source.txt"
    note.write_text("Deterministic synthetic metrics for MLBoard integration demonstration.\n")
    reports = []
    for group,model in (("run-series1","resnet18"),("run-series2","convnext")):
        for seed in (1,2):
            random_source = random.Random(seed)
            path = f"{group}/{model}/seed-{seed}"
            run = mlboard.init(args.project,path,config={"model":model,"seed":seed,"lr":0.01,"batch_size":128,"dataset":"synthetic-demo"},tags=["demo",model],spool_dir=spool,sync="background" if args.sync else "off",server=args.server,create_project=args.create_project)
            run.log_message("Starting explicit deterministic synthetic training demonstration")
            for step in range(args.steps):
                progress = step/max(1,args.steps-1)
                loss = 2.2*math.exp(-progress*4.2)+(0.13 if model=="convnext" else 0.2)+random_source.gauss(0,.028)
                accuracy = min(.99,0.34+.6*(1-math.exp(-progress*3.4))+random_source.gauss(0,.009))
                run.log({"train/loss":loss,"train/accuracy":accuracy-.015,"val/loss":loss+.1+0.05*math.sin(step*.3),"val/accuracy":accuracy,"system/learning_rate":.01*(1-progress*.9),"system/throughput":250+12*math.sin(step*.2)+seed},step=step,epoch=step/20,processed_samples=step*128)
                if args.pause:
                    time.sleep(args.pause)
            run.log_histogram("weights/layer-1",bin_edges=[-2,-1,0,1,2],counts=[2,17,20,3],step=args.steps-1)
            run.log_confusion_matrix("val/confusion",[[43,7],[4,46]],labels=["cat","dog"],step=args.steps-1)
            run.log_roc("val/roc",thresholds=[-math.inf,.3,.7,.9,math.inf],tp=[10,9,6,2,0],fp=[10,4,1,0,0],fn=[0,1,4,8,10],tn=[0,6,9,10,10],positive_class_id="cat",step=args.steps-1)
            run.log_text("notes/training","Synthetic CPU-generated measurements; no training dataset or remote secrets.",step=args.steps-1)
            run.log_table("val/examples",["label","prediction","confidence"],[["cat","cat",.94],["dog","dog",.89]],step=args.steps-1)
            run.log_media("files/demo-note",note,kind="file",step=args.steps-1)
            run.log_message("Finished synthetic training demonstration")
            receipt = run.finish(timeout=15,wait_for="remote" if args.sync else "local")
            reports.append({"run_id":run.id,"run_path":path,**asdict(receipt)})
    print(json.dumps({"logdir":str(spool),"runs":reports},indent=2))
    return 0 if all(r["complete"] for r in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
