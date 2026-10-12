"""GDB Python entry point: capture stacks without arguments, locals or core dumps."""

import json
import os
from pathlib import Path

import gdb


def main():
    output = Path(os.environ["DIAGNOSTIC_DIR"])
    output.mkdir(parents=True, exist_ok=True)
    state = {"signal": None, "inferior_exit_code": None}

    def on_stop(event):
        if isinstance(event, gdb.SignalEvent):
            state["signal"] = event.stop_signal

    def on_exit(event):
        state["inferior_exit_code"] = getattr(event, "exit_code", None)

    for command in (
        "set pagination off",
        "set confirm off",
        "set print frame-arguments none",
        "set print entry-values no",
        "set print thread-events off",
        "set debuginfod enabled off",
        "set disable-randomization off",
        "handle SIGPIPE nostop noprint pass",
        "handle SIGUSR1 nostop noprint pass",
        "handle SIGUSR2 nostop noprint pass",
        "handle SIGABRT stop print nopass",
        "handle SIGSEGV stop print nopass",
    ):
        gdb.execute(command)
    gdb.events.stop.connect(on_stop)
    gdb.events.exited.connect(on_exit)

    try:
        gdb.execute("run")
    except gdb.error as exc:
        state["debugger_error"] = str(exc)

    if state["signal"]:
        state["status"] = "signal_stopped"
        thread = gdb.selected_thread()
        state["crashing_thread"] = thread.num if thread else None
        for command in (
            "info threads",
            "bt 80",
            "thread apply all bt 80",
            "info sharedlibrary",
            "info proc mappings",
            "info files",
        ):
            print(f"\n=== {command} ===", flush=True)
            try:
                gdb.execute(command)
            except gdb.error as exc:
                print(f"Unavailable: {exc}", flush=True)
        exit_code = {"SIGABRT": 134, "SIGSEGV": 139}.get(state["signal"], 2)
        state["diagnostic_exit_code"] = exit_code
        # This is a detected stop, not a signal delivered through the original handler.
        state["status_note"] = "GDB intercepted the signal before delivery."
    else:
        exit_code = state["inferior_exit_code"]
        if exit_code is None:
            exit_code = 2
        state["status"] = "exited" if exit_code == 0 else "nonzero_exit"
        state["diagnostic_exit_code"] = exit_code

    (output / "gdb-status.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print("GDB_RESULT " + json.dumps(state), flush=True)
    gdb.execute(f"quit {exit_code}")


main()
