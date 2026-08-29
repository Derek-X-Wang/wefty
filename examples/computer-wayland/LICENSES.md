# Wayland reference image attribution and license manifest

This image contains only Debian `main` packages plus the components listed
below. Its build emits `/usr/share/wefty/licenses/debian-packages.tsv`, requires
every installed package to carry its Debian copyright file, and emits the
closed `/usr/share/wefty/licenses/non-dpkg-components.tsv` inventory for the
three non-package components.

| Component or idea | Version / identity | License | Use |
| --- | --- | --- | --- |
| Debian | trixie snapshot `20260827T000000Z` | DFSG-free package licenses | Base and desktop packages |
| wayvnc | `0.9.1-1` | ISC | Native RFB-over-WebSocket server |
| neatvnc | `0.9.1+dfsg-1`, upstream `cc19604c144bd5b9d33436f322e4679455ad6172` | ISC | Patched native WebSocket transport; patch retained in this repository |
| mise | `v2026.8.14` | MIT | Lazy agent CLI stubs |
| Herdr | `c2637dc182ddc5425108824d5ed15d24ce38c4e3` | Apache-2.0 | Inspiration for the visible agent-state surface only; no code or assets copied |
| Omarchy | research reference only | N/A | Keyboard-first and self-reconfiguration ideas only; no code, assets, installer, name, or branding copied |

The Wefty-authored scripts, configuration, HTML, and source patch are covered
by this repository's license. Chromium runs with `--no-sandbox`; that disclosed
image limitation does not add a privilege or weaken Wefty's OCI profile.
