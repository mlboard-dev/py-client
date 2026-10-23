"""MLBoard: bounded local experiment logging; no third-party runtime dependency."""
from .sdk import FinishReceipt, LogReceipt, Run, init
from .validation import ValidationError
__version__ = "0.1.0"
__all__ = ["init","Run","LogReceipt","FinishReceipt","ValidationError"]

from contextvars import ContextVar
_current = ContextVar("mlboard_current_run",default=None)
_init = init

def init(project, run_path, **kwargs):
    run = _init(project,run_path,**kwargs)
    _current.set(run)
    return run

def _run():
    run = _current.get()
    if run is None:
        raise RuntimeError("call mlboard.init() first")
    return run

def log(values, **coordinates):
    return _run().log(values,**coordinates)

def finish(**options):
    return _run().finish(**options)

def status():
    return _run().status()

__all__ += ["log","finish","status"]
