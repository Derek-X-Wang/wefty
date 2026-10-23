#!/usr/bin/env python3
"""Check one removed Seat's endpoint absence from its task root and kernel table."""
import os
from pathlib import Path
import sys


def main():
    root, table, place = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
    if place not in range(1, 5):
        raise ValueError('invalid Seat place')
    prefix = '/wefty/service/seats/%d/' % place
    for name in ('view.ws', 'control.ws', 'view.sock', 'control.sock'):
        path = root / (prefix + 'run/' + name).lstrip('/')
        try:
            os.lstat(path)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('removed Seat endpoint path remains: ' + name)
    # An unlinked listener or an accepted stream can outlive its pathname.
    # Match the full Seat prefix and both X tokens, like checkSeatUnixSockets.
    displays = ('/tmp/.X11-unix/X%d' % place, '@/tmp/.X11-unix/X%d' % place)
    for line in table.read_text().splitlines():
        fields = line.split()
        if len(fields) >= 8 and (fields[7].startswith(prefix) or fields[7] in displays):
            raise AssertionError('removed Seat Unix socket remains: ' + fields[7])
    print('absent')


if __name__ == '__main__':
    main()
