import threading
import traceback
import sys


def get_stacktrace_all_threads():
    stacktrace = ""
    for th in threading.enumerate():
        stacktrace += str(th)
        stacktrace += "".join(traceback.format_stack(sys._current_frames()[th.ident]))
    return stacktrace

def get_stacktrace():
    stacktrace = []
    th = threading.current_thread()
    stacktrace.extend(traceback.format_stack(sys._current_frames()[th.ident]))
    return stacktrace