"""Keep adlfs from installing fsspec's IO loop as the task thread's event loop.

adlfs (checked on 2026.8.0) calls asyncio.set_event_loop(<fsspec loop>) when it connects
from a thread with no running loop. That loop runs on fsspec's own IO thread. Any later
loop.run_until_complete() in the task thread then fails with "This event loop is already
running". pydantic-ai's Agent.run_sync, which every Common AI Provider operator uses, does
exactly that, so LLMFileAnalysisOperator cannot read a file from Azure storage without
this fix.

adlfs does its real work on fsspec's loop through fsspec.asyn.sync(), so it never needs
the calling thread's current loop. This makes set_event_loop a no-op inside adlfs only.
Python imports sitecustomize automatically at startup, so the fix applies to every Airflow
process in the image.
"""
try:
    import asyncio as _asyncio
    import types as _types

    import adlfs.spec as _spec

    _shim = _types.SimpleNamespace(**{k: getattr(_asyncio, k) for k in dir(_asyncio) if not k.startswith("__")})
    _shim.set_event_loop = lambda loop: None
    _spec.asyncio = _shim
except ImportError:
    pass
