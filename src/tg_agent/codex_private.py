"""Private files for the local Telegram delivery queue."""
import os
from pathlib import Path

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


