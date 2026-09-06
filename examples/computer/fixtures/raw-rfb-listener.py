#!/usr/bin/env python3
"""Fixture-only plain TCP listener that emits an RFB banner without WebSocket."""

import socket
import sys


port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen()
    while True:
        connection, _ = listener.accept()
        with connection:
            connection.sendall(b"RFB 003.008\n")
