#!/usr/bin/env python3
"""Run as the Seat uid: root intentionally lacks CAP_IPC_OWNER in wefty-v1."""
import ctypes
import errno
import os
from pathlib import Path


def clean_ipc():
    uid = os.getuid()
    if uid not in range(2001, 2005):
        raise RuntimeError('seat_uid_required')
    libc = ctypes.CDLL(None, use_errno=True)
    for kind, command in (('shm', libc.shmctl), ('msg', libc.msgctl), ('sem', libc.semctl)):
        rows = Path('/proc/sysvipc/' + kind).read_text().splitlines()
        if not rows:
            continue
        header = rows[0].split()
        identifier = header.index({'shm': 'shmid', 'msg': 'msqid', 'sem': 'semid'}[kind])
        for row in rows[1:]:
            fields = row.split()
            if uid not in (int(fields[header.index('uid')]), int(fields[header.index('cuid')])):
                continue
            number = int(fields[identifier])
            # IPC_RMID=0. semctl takes (semid, semnum, cmd), not shmctl's
            # (shmid, cmd, buffer). A fourth variadic argument is not needed.
            result = command(number, 0, 0)
            if result != 0 and ctypes.get_errno() not in (errno.EINVAL, errno.EIDRM):
                raise OSError(ctypes.get_errno(), 'Seat IPC cleanup failed')
    for entry in Path('/dev/mqueue').iterdir():
        if entry.lstat().st_uid == uid:
            entry.unlink()


if __name__ == '__main__':
    clean_ipc()
