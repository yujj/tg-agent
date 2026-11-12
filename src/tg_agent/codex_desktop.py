#!/usr/bin/env python3
"""Codex Desktop IPC, adapted from the owner's singularity babysit-thread.

Internal protocol, probed at subscription time. No inference or Telegram calls here.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import stat
import struct
import sys
import time
import uuid

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace", newline="\n")

def private_path(path: Path):
    """Replace permissions with current-user/System access before storing chat text."""
    if os.name != "nt":
        path.chmod(0o700 if path.is_dir() else 0o600)
        return
    import ctypes
    from ctypes import wintypes as w

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = (w.HANDLE,)
    kernel.LocalFree.argtypes = (w.HLOCAL,)
    advapi.OpenProcessToken.argtypes = (w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE))
    advapi.GetTokenInformation.argtypes = (w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD))
    advapi.ConvertSidToStringSidW.argtypes = (w.LPVOID, ctypes.POINTER(w.LPWSTR))
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = (
        w.LPCWSTR, w.DWORD, ctypes.POINTER(w.LPVOID), ctypes.POINTER(w.DWORD))
    advapi.GetSecurityDescriptorDacl.argtypes = (
        w.LPVOID, ctypes.POINTER(w.BOOL), ctypes.POINTER(w.LPVOID), ctypes.POINTER(w.BOOL))
    advapi.SetNamedSecurityInfoW.argtypes = (
        w.LPWSTR, ctypes.c_int, w.DWORD, w.LPVOID, w.LPVOID, w.LPVOID, w.LPVOID)
    advapi.SetNamedSecurityInfoW.restype = w.DWORD
    token = w.HANDLE()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        size = w.DWORD()
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        if not size.value:
            raise ctypes.WinError(ctypes.get_last_error())
        user = ctypes.create_string_buffer(size.value)
        if not advapi.GetTokenInformation(token, 1, user, size, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        sid = w.LPWSTR()
        if not advapi.ConvertSidToStringSidW(ctypes.c_void_p.from_buffer(user).value, ctypes.byref(sid)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            flags = "OICI" if path.is_dir() else ""
            sddl = f"D:P(A;{flags};FA;;;SY)(A;{flags};FA;;;{sid.value})"
        finally:
            kernel.LocalFree(ctypes.cast(sid, w.HLOCAL))
    finally:
        kernel.CloseHandle(token)
    descriptor = w.LPVOID()
    if not advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1, ctypes.byref(descriptor), None):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        present, defaulted, dacl = w.BOOL(), w.BOOL(), w.LPVOID()
        if not advapi.GetSecurityDescriptorDacl(descriptor, ctypes.byref(present), ctypes.byref(dacl),
                                              ctypes.byref(defaulted)) or not present:
            raise ctypes.WinError(ctypes.get_last_error())
        error = advapi.SetNamedSecurityInfoW(str(path), 1, 0x80000004, None, None, dacl, None)
        if error:
            raise ctypes.WinError(error)
    finally:
        kernel.LocalFree(descriptor)


def private_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    private_path(path)


def private_file(path: Path):
    # The containing directory is already private; even the empty file inherits
    # no read access for other users before its explicit permissions are set.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.close(descriptor)
    private_path(path)


class DeliveryError(Exception):
    def __init__(self, message: str, *, uncertain: bool = False):
        super().__init__(message)
        self.uncertain = uncertain


def default_socket() -> Path:
    if os.name == "nt":
        return Path(r"\\.\pipe\codex-ipc")
    return Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "ipc" / "ipc.sock"


def verify_pipe_owner(handle):
    """Reject a pipe server running as another Windows user before sending data."""
    import ctypes
    from ctypes import wintypes as w

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetNamedPipeServerProcessId.argtypes = (w.HANDLE, ctypes.POINTER(w.ULONG))
    kernel.GetNamedPipeServerProcessId.restype = w.BOOL
    kernel.OpenProcess.argtypes = (w.DWORD, w.BOOL, w.DWORD)
    kernel.OpenProcess.restype = w.HANDLE
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = (w.HANDLE,)
    advapi.OpenProcessToken.argtypes = (w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE))
    advapi.GetTokenInformation.argtypes = (w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD))
    advapi.EqualSid.argtypes = (w.LPVOID, w.LPVOID)
    advapi.EqualSid.restype = w.BOOL

    def token_user(process):
        token = w.HANDLE()
        if not advapi.OpenProcessToken(process, 8, ctypes.byref(token)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            size = w.DWORD()
            advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if not size.value:
                raise ctypes.WinError(ctypes.get_last_error())
            buffer = ctypes.create_string_buffer(size.value)
            if not advapi.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)):
                raise ctypes.WinError(ctypes.get_last_error())
            return buffer
        finally:
            kernel.CloseHandle(token)

    pid = w.ULONG()
    if not kernel.GetNamedPipeServerProcessId(handle, ctypes.byref(pid)):
        raise ctypes.WinError(ctypes.get_last_error())
    process = kernel.OpenProcess(0x1000, False, pid.value)
    if not process:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        server = token_user(process)
        current = token_user(kernel.GetCurrentProcess())
        if not advapi.EqualSid(ctypes.c_void_p.from_buffer(server).value,
                               ctypes.c_void_p.from_buffer(current).value):
            raise DeliveryError("Codex IPC pipe server must belong to the current user")
    finally:
        kernel.CloseHandle(process)


class WindowsPipe:
    """Socket-like byte transport with cancellable Windows overlapped I/O."""

    def __init__(self, path, timeout):
        import _winapi

        self.api = _winapi
        self.timeout = timeout
        address = str(path)
        if not address.lower().startswith("\\\\.\\pipe\\"):
            raise DeliveryError("Codex IPC requires a local Windows named pipe")
        try:
            self.handle = _winapi.CreateFile(address, _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                                             0, 0, _winapi.OPEN_EXISTING,
                                             _winapi.FILE_FLAG_OVERLAPPED, 0)
        except OSError as exc:
            if exc.winerror != _winapi.ERROR_PIPE_BUSY:
                raise
            _winapi.WaitNamedPipe(address, max(1, int(timeout * 1000)))
            self.handle = _winapi.CreateFile(address, _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                                             0, 0, _winapi.OPEN_EXISTING,
                                             _winapi.FILE_FLAG_OVERLAPPED, 0)
        try:
            verify_pipe_owner(self.handle)
        except BaseException:
            self.close()
            raise

    def settimeout(self, timeout):
        self.timeout = timeout

    def _io(self, operation, value):
        pending, error = operation(self.handle, value, overlapped=True)
        if error == self.api.ERROR_IO_PENDING:
            wait = self.api.WaitForSingleObject(pending.event, max(1, int(self.timeout * 1000)))
            if wait != self.api.WAIT_OBJECT_0:
                pending.cancel()
                pending.GetOverlappedResult(True)
                raise TimeoutError("Codex IPC named pipe timed out")
        count, error = pending.GetOverlappedResult(True)
        if error:
            raise OSError(error, "Codex IPC named pipe I/O failed")
        return pending, count

    def sendall(self, data):
        deadline = time.monotonic() + self.timeout
        offset = 0
        while offset < len(data):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Codex IPC named pipe write timed out")
            previous, self.timeout = self.timeout, remaining
            try:
                _, count = self._io(self.api.WriteFile, data[offset:])
            finally:
                self.timeout = previous
            if not count:
                raise ConnectionError("Codex IPC named pipe closed")
            offset += count

    def recv(self, size):
        pending, count = self._io(self.api.ReadFile, size)
        return pending.getbuffer()[:count]

    def close(self):
        if self.handle is not None:
            self.api.CloseHandle(self.handle)
            self.handle = None


class Desktop:
    """Length-prefixed JSON IPC, not the public app-server JSON-RPC transport."""

    def __init__(self, path: Path | None = None, timeout: float = 15):
        self.path = path or default_socket()
        self.timeout = timeout
        self.sock = None
        self.client_id = "initializing-client"

    def __enter__(self):
        if os.name == "nt":
            self.sock = WindowsPipe(self.path, self.timeout)
        else:
            info = self.path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                raise DeliveryError("Codex IPC socket must belong to the current user")
            if self.path.parent.stat().st_mode & 0o022:
                raise DeliveryError("Codex IPC directory is writable by another user")
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
        try:
            if os.name != "nt":
                self.sock.connect(str(self.path))
            response = self.request("initialize", {"clientType": "tg-agent"}, 0)
            self.client_id = response["result"]["clientId"]
        except BaseException:
            self.sock.close()
            raise
        return self

    def __exit__(self, *_):
        if self.sock:
            self.sock.close()

    def _read(self, size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            part = self.sock.recv(size - len(data))
            if not part:
                raise ConnectionError("Codex IPC closed")
            data.extend(part)
        return bytes(data)

    def request(self, method: str, params: dict, version: int, *, target=None, mutation=False):
        request_id = str(uuid.uuid4())
        request = {
            "type": "request", "requestId": request_id,
            "sourceClientId": self.client_id, "version": version,
            "method": method, "params": params, "timeoutMs": int(self.timeout * 1000),
        }
        if target:
            request["targetClientId"] = target
        data = json.dumps(request, ensure_ascii=False).encode("utf-8")
        sent = False
        deadline = time.monotonic() + self.timeout
        try:
            # A partial send may already have reached Desktop. Never blindly retry it.
            sent = True
            self.sock.sendall(struct.pack("<I", len(data)) + data)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Codex IPC response timed out")
                self.sock.settimeout(remaining)
                size = struct.unpack("<I", self._read(4))[0]
                if not 0 < size <= 16 * 1024 * 1024:
                    raise ValueError("Unexpected Codex IPC frame size")
                response = json.loads(self._read(size))
                if response.get("type") != "response" or response.get("requestId") != request_id:
                    continue
                if response.get("resultType") != "success":
                    error = response.get("error", "Invalid Codex IPC response")
                    # Only no-client-found proves the router did not hand off the mutation.
                    raise DeliveryError(str(error), uncertain=mutation and error != "no-client-found")
                return response
        except DeliveryError:
            raise
        except (OSError, ValueError) as exc:
            raise DeliveryError(str(exc), uncertain=mutation and sent) from exc

    def owner(self, thread_id: str) -> str:
        response = self.request("thread-owner-discovery", {"hostId": "local", "conversationId": thread_id}, 1)
        if response.get("result", {}).get("supportsUntrustedAppInput") is not True:
            raise DeliveryError("Desktop owner lacks the expected event-input capability")
        return response["handledByClientId"]

    def deliver(self, thread_id: str, event_id: str, output: str, *,
                name: str = "telegram_event", source: str = "tg-agent") -> dict:
        owner = self.owner(thread_id)
        tool_output = {"name": name, "namespace": None, "output": output}
        try:
            response = self.request("thread-follower-steer-turn", {
                "conversationId": thread_id, "input": [], "toolOutput": tool_output,
                "clientUserMessageId": event_id, "attachments": [],
                "restoreMessage": {"id": event_id, "text": output, "createdAt": int(time.time() * 1000),
                    "context": {"prompt": output, "addedFiles": [], "fileAttachments": [],
                                "imageAttachments": [], "ideContext": None, "workspaceRoots": []}},
            }, 1, target=owner, mutation=True)
        except DeliveryError as exc:
            inactive = f"Cannot steer conversation {thread_id} because its active turn already ended"
            if str(exc) not in {inactive, f"SteerTurnInactiveError: {inactive}"}:
                raise
        else:
            turn_id = response.get("result", {}).get("result", {}).get("turnId")
            if not turn_id:
                raise DeliveryError("Desktop steering returned no turn receipt", uncertain=True)
            return {"turn": {"id": turn_id}}

        # Real untrusted input activates Desktop's pre-start busy guard. Using
        # toolOutput on raw start during an active turn leaves an orphan UI turn.
        call_id = "telegram_" + event_id.replace("-", "")
        items = [
            {"type": "function_call", "call_id": call_id, "name": "untrusted_input", "arguments": "{}"},
            {"type": "function_call_output", "call_id": call_id,
             "output": [{"type": "input_text", "text": json.dumps({"source": source, "text": output})}]},
        ]
        try:
            response = self.request("thread-follower-start-turn", {
                "conversationId": thread_id,
                "turnStart": {"request": {"threadId": thread_id, "input": [], "clientUserMessageId": event_id},
                              "context": {"responseItems": items}},
            }, 2, target=owner, mutation=True)
        except DeliveryError as exc:
            busy = "App context must wait until the current turn finishes"
            if str(exc) in {busy, "Error: " + busy}:
                raise DeliveryError(busy) from exc  # rejected before history/UI mutation
            raise  # injection may have succeeded: never replay this blindly
        result = response.get("result", {}).get("result", {})
        if not result.get("turn", {}).get("id"):
            raise DeliveryError("Desktop accepted IPC without a turn receipt", uncertain=True)
        return result


