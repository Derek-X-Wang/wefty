---
name: self-reconfigure-desktop
description: Change this image's user-owned visual theme without changing the Computer contract or privileged runtime.
---

# Self-reconfigure this desktop

Use `wefty-theme graphite` or `wefty-theme amber` to atomically update
`$HOME/.config/wefty/theme.json`. The focused desktop surface reopens that file
and applies the supported theme; the setting persists because `$HOME` belongs
to `/wefty/service`.

Do not edit `/wefty/control`, listener ports, the compositor backend, or the
view/control roles. Those are Computer contract inputs, not theme settings.
