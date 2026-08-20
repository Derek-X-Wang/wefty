# Lima/containerd cross-boundary prototype

Throwaway evidence harness for issue #104. It drives rootful containerd in a
Lima `vz` VM from a native macOS Go process through Lima's forwarded Unix
socket.

The checked-in `lima.yaml` is deliberately specific to the one authorized
`wefty-proto` instance and this worktree. Generated logs, tarballs, binaries,
and writable-mount test data are ignored.

```sh
mkdir -p shared out bin
limactl start --name wefty-proto ./lima.yaml
go build -o bin/wefty-lima-proto .
bin/wefty-lima-proto run \
  -socket "$HOME/.lima/wefty-proto/sock/containerd.sock" \
  -host-shared "$PWD/shared" \
  -guest-shared /mnt/wefty-proto \
  -out "$PWD/out"
```

The restart rows use explicit two-process modes so the evidence is not faked
inside one process:

```sh
bin/wefty-lima-proto client-restart-prepare ...
bin/wefty-lima-proto client-restart-resume ...
bin/wefty-lima-proto vm-restart-prepare ...
limactl stop wefty-proto && limactl start wefty-proto
bin/wefty-lima-proto vm-restart-resume ...
```
