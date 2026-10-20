# Reaching wefty while the laptop's Tailscale app is on another account

Date: 2026-10-20. Method: primary documentation and pinned upstream source, public reads only. No account, tailnet, runtime experiment, product-code change, commit, or issue comment. Pinned revisions: wefty `837ace2ad59f03063adb5d3ed3438978d157ed70`; `tailscale.com` v1.104.1 (`9a522a97`, the version in wefty's `go.mod` and the latest release on the Go proxy); Headscale v0.29.5 (2026-10-13) and unreleased `main` at `691ef563`; NetBird v0.81.3; Nebula v1.11.2. [W-go] [GoProxy-ts] [HS-releases]

**Question.** A laptop's Tailscale app switches between a personal and a work account (two tailnets, one active at a time). wefty's machines live on the personal tailnet. Can the owner keep using wefty, and open web apps that wefty hosts on another machine, while the app is on the work account, and get everything back after switching back? Which approach is best, given that wefty may later replace Tailscale with Headscale or its own network?

**Answer.** Use wefty's own embedded connection for this, not the OS app. Each wefty program runs its own `tsnet` node with its own state, keys, and userspace network stack, so the app's active account does not decide whether wefty works. [TS-tsnet-doc] [W-tsnet] The planned `wefty open <service>` local forward is the right path for hosted web apps. It needs only `Fabric.Dial`, so it carries over unchanged to Headscale or an own relay. The "machine name + port" path keeps working only while the OS app is on the personal tailnet, and on any future network it needs an OS-level client of that network on every device. Node sharing, a second `tailscaled`, Tailscale Services and Funnel all fail at least one requirement: owner-only access, working during the switch, or portability. **The one material unknown is runtime.** On macOS, the embedded node tries to keep its traffic off the app's tunnel. When the work account selects an exit node, that depends on a macOS "delegated interface" value that Tailscale's own code calls unreliable. The attended run must test this. [TS-netns-darwin] [TS-defaultroute-darwin]

**Evidence legend.** **VERIFIED** = observed in cited documentation or pinned source, not runtime-tested. **UNVERIFIED** = inference, design proposal, or behaviour not shown by a source. **NOT-RUN** = no runtime proof was collected in this research.

## 1. How the Tailscale app switches accounts

| Fact | Status |
|---|---|
| "A Tailscale client device lets only one active account log in at a time", and "A device is not able to transmit packets on multiple tailnets simultaneously." Fast user switching is on all plans, on macOS, iOS, Windows and Android apps, and the Linux/macOS/Windows CLI (`tailscale switch`). Page last validated 2025-12-04. | VERIFIED [TS-fus] |
| Switching back does not ask to sign in again unless the account was never used on this device or the device's node key for that tailnet has expired. | VERIFIED [TS-fus] |
| The request to connect to several tailnets at once (tailscale#183, opened 2020) is still **open** (last update 2026-05-18). No 2025–2026 change gives one device two active tailnets. | VERIFIED [TS-183] |
| "Multiple tailnets" (alpha, validated 2026-02-04) lets an organization run several tailnets under one identity provider. It adds a device to one selected tailnet. It is not multi-homing. | VERIFIED [TS-multi-tailnet] |
| On a profile switch, the open-source backend replaces its node state, clears the network map and packet filter, and shuts down the old control client (`resetForProfileChangeLocked`). Inference: open connections from the OS to old-tailnet peers stop. Browser tabs on `http://machine:port` must reload after switching back. | VERIFIED source [TS-switch]; that the macOS GUI shell uses this exact path is UNVERIFIED (the shell is not in the public repo) |
| The fast-user-switching page does not describe what happens to live connections. | VERIFIED (absence) [TS-fus] |

The account list in the macOS app can also hold a self-hosted control server: Headscale documents "Option-click → Debug → Custom Login Server → Add Account…". A future Headscale tailnet would be one more entry in the same one-at-a-time switcher. [HS-apple]

## 2. Is wefty's embedded node independent of the app?

**Own identity, own state, own stack — VERIFIED.** `tsnet` "runs a fully self-contained Tailscale node inside your process using a userspace TCP/IP stack (gVisor)". It needs no root and no system daemon, and the node's identity and state live in a directory the program controls. Unless `Store` is set, state is a `FileStore` at `Dir/tailscaled.state`. wefty sets `Dir` explicitly per program, so the node agent, the CLI, and the person identity are separate devices with separate keys. [TS-tsnet-src L10-L20] [TS-tsnet-pkg] [W-tsnet]

**Names and peer addresses do not use the OS — VERIFIED.** `Server.Dial` calls `tsdial.Dialer.UserDial`, which first resolves the name from *this node's* network map ("MagicDNS or otherwise baked into the NetworkMap? Try that first"). Any address that belongs to a peer of this node is then dialled through the in-process netstack (`UseNetstackForIP`), not through the OS routing table. The app's MagicDNS and its 100.64.0.0/10 routes for the work tailnet therefore do not affect how wefty reaches its own peers. [TS-tsnet-dial] [TS-tsdial-resolve] [TS-tsdial-userdial] [TS-tsnet-engine]

**Ports — VERIFIED.** `Server.Port` is the WireGuard/peer-to-peer UDP port; "If zero, a port is automatically selected." wefty does not set it, so it cannot collide with the app's port. [TS-tsnet-port] [W-tsnet]

**Underlay traffic and the work account's exit node — VERIFIED source, NOT-RUN.** The embedded node's own internet traffic is its control connection, its DERP relay connections, and its UDP to peers. The control and DERP clients dial through `netns.NewDialer`. Peer UDP listens through `netns.Listener`. [TS-magicsock] [TS-derphttp] On macOS, `netns.controlLogf` binds each such socket to one interface with `IP_BOUND_IF`. The interface is the default-route interface. When the default route is a `utun` tunnel, netmon takes the tunnel's *delegated* physical interface (`SIOCGIFDELEGATE`). There is also an explicit check that refuses any `utun` holding a Tailscale 100.x address. The code comment says "with an exit node enabled, this is almost always true". [TS-netns-darwin] [TS-bsd-defaultroute] [TS-darwin-delegate] What this means:

- Normal case (inference): wefty's encrypted traffic leaves on the physical interface and **does not pass through the work tailnet's exit node**. The OS app's choices do not break it.
- Weak spot (UNVERIFIED): Tailscale's own comment says "We cannot rely on the delegated interface data on darwin". The Network Extension sets it only when the tunnel is created, and the macOS app works around this with Swift-side path updates that a plain Go program does not get. [TS-defaultroute-darwin] If the lookup fails, the code finds the Tailscale `utun`, marks the state invalid, and leaves the socket **unbound** (`controlLogf` returns nil on error). The socket then follows the OS routes, through the work exit node, or fails if that path blocks it.
- Corollary for self-hosting (VERIFIED, Linux wording): Headscale's test-kit notes that an unprivileged tsnet "binds its sockets to the default-route interface". A control server reachable only through another interface is then unreachable "on multi-homed production hosts". The documented fixes are `CAP_NET_ADMIN` or `netns.SetEnabled(false)`. [HS-nix]

**Other caveats.**
- Tailscale warns that full-tunnel exit nodes "only support one VPN at a time" and that Tailscale sets "aggressive firewall rules" for exit nodes. The page does not say whether this applies to the macOS app. UNVERIFIED for macOS. [TS-other-vpns]
- The app and the open-source `tailscaled` must not be mixed: "Do not install the Mac App Store variant and the Standalone variant on the same machine." This is about GUI variants. No page addresses `tsnet` next to the app. Coexistence is supported by architecture (userspace, no TUN, no system extension), not by a documented guarantee. [TS-macos-variants]
- Name lookups for the control and DERP hostnames still go through the macOS resolver, which the work account's DNS settings may control. UNVERIFIED whether a restrictive work DNS policy could delay the embedded node's start.
- `tsnet.go` v1.104.1 has no reference to system policy. The open-source policy stores are env, JSON and Windows registry. Inference: device-management policies that target the app (forced exit node, always-on) do not reach an embedded node. UNVERIFIED at runtime. [TS-tsnet-src] [TS-syspolicy]

## 3. Ways to reach a service on the personal tailnet while the app is on the work tailnet

| Approach | Works during the switch? | Only the owner's devices? | Fit |
|---|---|---|---|
| **a. `wefty open` local forward** (localhost listener → `Fabric.Dial` as the owner's person identity) | Yes. Independent of the app (§2) | Far end: yes, the dial comes from the owner's person-identity node. Near end: any local process that can reach the port | **Keep (primary)** |
| a′. `tsnet.Server.Loopback()` SOCKS5 + LocalAPI | Yes | Exposes the whole tailnet and a write-enabled LocalAPI to whoever holds the credentials | Reject for browsers |
| Machine name + port through the OS app | **No** (only while the app is on personal) | Yes, through tailnet policy | Keep as the convenience path |
| b. Node sharing into the work tailnet | Yes | Shared to the owner's *work identity*, inside a tailnet that someone else may administer | Reject |
| c. Second userspace `tailscaled` on the Mac | Yes, in principle | Yes | Reject (repeats a.) |
| d. Services / app connectors / Serve | No (same tailnet only) | — | Not applicable |
| Funnel | — | **No: public internet** | Excluded |

**a. Local forward through the embedded node.** `wefty open` listens on `127.0.0.1:<port>` and carries each connection through `Fabric.Dial` from the laptop's person-identity node. The node agent's published port sees an ordinary tailnet peer owned by the owner, so the existing owner-only rule still applies at the far end. Nothing in the path depends on the OS app (§2). Design caveats (UNVERIFIED design notes, standard web behaviour):
- Anything on the laptop that can open the local port gets through, including other local accounts and web pages that send requests to `localhost`. Bind loopback only, and do not use `0.0.0.0`.
- "Cookies do not provide isolation by port." Several hosted apps on `localhost:<port>` share one cookie jar. [RFC6265-8.5]
- Apps that build absolute URLs or check `Host` see `localhost:<port>`, not the machine name.
- Choose stable ports per service so bookmarks still work, and report a port collision clearly.

**a′. `tsnet` `Loopback()`.** It starts a `127.0.0.1:0` listener that serves SOCKS5 (user `tsnet`, random password) and the LocalAPI with `PermitWrite = true`. The password is new each time the process starts. [TS-tsnet-loopback] Chrome supports no SOCKS5 authentication methods ("No authentication methods are supported for SOCKSv5 in Chrome"), so a browser cannot use it directly. [Chromium-proxy] It also offers the whole tailnet, not one service, and it is Tailscale-only. Its place is in-process tools, as the 2026-08-06 synthesis already says. [Synthesis-§6]

**b. Node sharing.** A shared machine is "visible only to the individual recipient user". It appears to that user as `<hostname>.<tailnet-name>.ts.net`. It is quarantined: it "can receive incoming connections … but cannot start connections". Sharing is on all plans and can be revoked. [TS-sharing] It does not fit wefty:
- The recipient lives in the work tailnet. Accepting needs that tailnet's "Owner, Admin, or IT admin" role, and both tailnets' policies apply. The personal machine becomes visible in a tailnet that the owner may not administer. [TS-sharing]
- Tags are stripped, and "a machine cannot be shared with a tag or accessed by tagged machines on another tailnet". wefty's node agents are tagged machine identities, so whether they can be shared at all is UNVERIFIED. [TS-sharing] [W-tsnet]
- Every service needs its own share and its own long name. Headscale has no sharing: it implements "a _single_ Tailscale network". [HS-faq]

**c. A second userspace `tailscaled`.** `tailscaled` accepts `--tun=userspace-networking`, `--statedir`, `--socket` and `--port`, and can run a SOCKS5 or HTTP proxy. [TS-tailscaled-src] [TS-tailscaled-kb] [TS-userspace] So a second personal-tailnet daemon on the Mac is possible in principle. The browser would then need proxy settings. Tailscale documents neither more than one instance nor this layout on macOS next to the GUI app (UNVERIFIED). On macOS the open-source `tailscaled` "can advertise as exit node but cannot use them". [TS-macos-variants] It gives the same independence that wefty's own embedded node already has, adds a daemon to manage, carries no wefty identity, and is Tailscale-only.

**d. Other first-party features.**
- Tailscale Services: hosts must be tag-based (`ErrUntaggedServiceHost` in tsnet), and clients reach them inside the same tailnet. [TS-services] [TS-tsnet-svc] Headscale has no Services (request #2845 open). [HS-2845]
- App connectors: route domains through a connector device for clients of the same tailnet. [TS-app-connectors]
- Serve: publishes within the tailnet. Headscale lists Serve as unsupported in v0.29.5. Its unreleased `main` adds HTTPS via `tailscale serve`. [HS-features] [HS-changelog-main]

None of these reach across tailnets. **Funnel** is excluded because it routes "traffic from the broader internet" so a service is there "for anyone to access". That breaks the owner-only rule. [TS-funnel]

## 4. Compatibility with Headscale or an own network

### 4a. Headscale today

| Need | Headscale v0.29.5 | Source |
|---|---|---|
| `tsnet` against a custom control URL | Yes. Headscale's test kit documents tsnet with `ControlURL` + `TS_AUTHKEY` (`main`); there is no tsnet page in the v0.29.5 docs. Minimum client v1.80.0 (v1.84.0 on unreleased `main`); support window = "last 10 releases" | [HS-nix] [HS-rel-0290] [HS-clients] |
| Tags, tag ownership | Yes. A tagged node becomes owned by the special user `tagged-devices` | [HS-features] [HS-registration] |
| ACL policy, grants, autogroups, tests | ACLs yes; grants since v0.29.0; "some" autogroups | [HS-features] [HS-rel-0290] |
| Pre-auth keys: single-use, reusable, expiry, ephemeral, tags | Yes: `--reusable`, `--ephemeral`, `--expiration` (default single use, 1 hour), `--tags` | [HS-preauthkeys] [HS-registration] |
| OAuth client secret as auth key (tsnet self-mints tagged keys) | **Unreleased** (`main`, 0.30.0): `tskey-client-…?baseURL=<headscale>` | [HS-changelog-main] |
| OIDC login for a person identity | Yes ("Basic registration", profile update). **OIDC groups cannot be used in ACLs** | [HS-features] |
| `WhoIs` | Client side (LocalAPI over the network map). Headscale fills `StableID` from the node ID and `UserProfile.ID` from the user ID: the fields wefty reads | [W-tsnet] [HS-node] [HS-users] |
| Ephemeral nodes, MagicDNS, embedded DERP, peer relays, exit nodes, Taildrop | Yes | [HS-features] |
| Funnel, Serve, network flow logs | No (Serve/HTTPS certs on unreleased `main`) | [HS-features] [HS-changelog-main] |
| Services (`svc:`), node sharing, several tailnets | No / not in scope ("a _single_ Tailscale network") | [HS-2845] [HS-faq] |

wefty-specific check (UNVERIFIED with Headscale): `tsnet.Fabric.issuingFabricID` fails closed with `ErrIssuingFabricUnavailable` when `Status.CurrentTailnet.MagicDNSSuffix` is empty. A Headscale deployment for wefty therefore needs MagicDNS with a `base_domain`. Its `FabricID` will also differ from the Tailscale one, by design. [W-tsnet]

### 4b. Other overlays with an embeddable, no-root client

| Overlay | Self-host | In-process, no root | Peer identity for the app | NAT traversal / relay | Notes |
|---|---|---|---|---|---|
| **NetBird** v0.81.3 | Yes (management, signal, relay: AGPLv3; client: BSD-3) | **Yes**: `client/embed` gives `Dial`, `ListenTCP`, `ListenUDP` on wireguard-go netstack. It turns netstack on through process-wide `os.Setenv` | Weak: `IdentityForIP` returns WireGuard key + FQDN only; peer `State` has no user field | ICE (pion) + relay service | Login by setup key or JWT. [NB-embed] [NB-status] [NB-license] [NB-readme] |
| **Nebula** v1.11.2 | Yes (own CA + lighthouses, no control server) | **Yes**: `service` package (gVisor) with `Dial`/`Listen` (TCP, wildcard only) | Good: `Control.GetCertByVpnIp` returns the peer's signed cert (name, groups) | Lighthouse hole punching; relays marked "EXPERIMENTAL" | MIT. CA lifetime 1 year by default. [Neb-service] [Neb-control] [Neb-config] [Neb-readme] |
| **ZeroTier** libzt | Controller is in ZeroTierOne `nonfree/` (source-available) | C library with C#, Python, Rust and Java bindings. **No Go binding listed** (cgo would be needed) | Node address (UNVERIFIED) | UNVERIFIED in this pass | BSL 1.1: some commercial uses need a licence. [ZT-libzt] [ZT-one] |
| **Netmaker** v1.7.0 | Yes (Apache-2.0 core + pro licence) | No embeddable library found (UNVERIFIED); `netclient` daemon, kernel WireGuard | — | UNVERIFIED (README lists "Remote Access Gateways") | [NM-readme] |
| **innernet** v2.0.0 | Yes (coordination server) | No: Rust CLI, `sudo` throughout, kernel WireGuard or wireguard-go | CIDR-based | Coordination server "provide[s] endpoint information" for direct connections; no relay documented | [Inn-readme] |
| **wireguard-go netstack** | n/a (a library) | **Yes**: `CreateNetTUN` → `Net.DialContext`, `ListenTCP`, … | None (WireGuard public key only) | **None.** Endpoint discovery, NAT traversal and relay must be built; Tailscale's `cmd/derper` is an existing relay design | [WG-netstack] [GoProxy-wg] [TS-derp] |

Inference: only tsnet (Tailscale or Headscale control) gives wefty a person + device identity from `WhoIs` with no extra lookup. Nebula is the closest identity model (signed certificate per host). NetBird embeds well but would need a management-API lookup for person identity. An own relay built on wireguard-go must design identity itself, as the 2026-08-06 synthesis already concluded. [Synthesis-§6]

### 4c. Does each access approach survive a network swap?

| Approach | Tailscale → Headscale | → NetBird/Nebula/own relay | Why |
|---|---|---|---|
| `wefty open` local forward | **Yes, unchanged** | **Yes**, given a `Fabric` that implements `Dial` | Uses only `Fabric.Dial` on the laptop's person identity. Confirmed: `Fabric` is `Listen`/`Dial`/`WhoIs`/`ConnectHost` with no Tailscale types. [W-fabric] |
| Machine name + port | Yes, if the OS app is logged into the Headscale account (one more entry in the same switcher) | Only if every device runs an OS client of *that* network; names change | `ConnectHost` is the network's own hostname. [W-fabric] [HS-apple] |
| Node sharing | **No** (single tailnet) | No | [HS-faq] |
| Second `tailscaled` | Yes (`--login-server`) | No | Tailscale-specific |
| Services / Serve | No / not released | No | [HS-2845] [HS-features] |

Confirmed: the forward needs only `Dial`, and the machine-name path needs an OS-level client of the same network on every device.

## 5. Plan terms (brief, neutral)

- Pricing page: the Personal plan is "$0 for up to 6 users", "Unlimited user devices", "Up to 50 tagged resources", and "1,000 mins per month for ephemeral resources". Its FAQ says: "This is a free plan and is only suitable for non-commercial use of Tailscale". [TS-pricing]
- Pricing FAQ: a tailnet on a public email domain "is treated as personal use". A tailnet on a custom domain is "considered business use". Seats count users, not devices. A tagged resource is "a device that is owned by a tag rather than a user identity". [TS-pricing-faq]
- Terms of Service (updated 2026-08-25): access is "solely for your own personal use or internal business purposes (as applicable depending on your Plan)". The terms do not use the word "non-commercial". [TS-terms]
- No official page addresses a work-owned device joining a personal tailnet. If one ever did, the governing text would be the plan FAQ's "non-commercial use" line above, plus whatever applies on the work side. Not assessed here.
- Each wefty program on the laptop is one more *user device* on the personal tailnet (free, unlimited). Tagged node agents count against the 50 tagged resources. [TS-pricing]

## Recommendation for wefty

1. **Keep `wefty open` as the main path off the personal tailnet.**
   - Build it on `Fabric.Dial` from the person identity, so it keeps working while the app is on another account and survives a later move to Headscale or an own relay.
   - Bind `127.0.0.1` only, use a stable port per service, and print the URL.
   - Treat local exposure and the shared `localhost` cookie jar as documented limits. Do not try to solve them with tailnet features.
2. **Keep machine name + port** as the convenience path when the OS app is on the personal tailnet. Document that it stops while the app is on another account, and that tabs reload after switching back.
3. **Do not adopt** node sharing, a second `tailscaled`, `Loopback()` for browsers, Tailscale Services, or Funnel for this need. Each adds a dependency on Tailscale or another tailnet, and none adds anything the forward lacks.
4. **Attended real-network run (NOT-RUN today). Verify:**
   - With the app on the work account, `wefty` status and `wefty open` work. Then select an exit node in the work account and repeat. Read the embedded node's logs for which interface it binds to (e.g. `netns: …`). Check whether its path is direct or DERP, and whether the traffic bypasses the exit node.
   - Keep a long download or WebSocket open through `wefty open` while switching work→personal→work. Record any stall when the app's `utun` goes down and comes back.
   - Change network (Wi-Fi off/on, hotspot) while the work exit node is on. This is the stale-delegated-interface case. [TS-defaultroute-darwin]
   - After switching back, machine name + port works without signing in again.
   - Check that a local listener on the chosen port answers only on loopback.
5. **Keep the path to Headscale or an own network open:**
   - Keep `Fabric` free of Tailscale types, and add no feature that needs Services, sharing, Funnel, or Serve.
   - Plan a Headscale smoke test: MagicDNS with `base_domain` (for `issuingFabricID`), tagged pre-auth keys for agents, and OIDC login for the person identity. Use the Headscale NixOS test kit when 0.30 ships. [HS-changelog-main]
   - Before relying on a self-hosted control URL that is reachable only on a non-default interface, decide whether wefty should call `netns.SetEnabled(false)`. [HS-nix]

## Open questions

- Does the macOS app turn on the system's "route all traffic" tunnel mode (`includeAllNetworks`) for exit nodes? If it does, sockets bound with `IP_BOUND_IF` may still be forced into the tunnel. No primary source found. The attended run answers it in practice.
- Can a tagged wefty node be shared at all? This is moot if sharing is rejected.
- Should `wefty open` also check `Host` against DNS-rebinding pages? This is a product decision, not researched here.

## Sources

Tailscale (docs; source pinned at v1.104.1):

[TS-fus]: https://tailscale.com/kb/1225/fast-user-switching
[TS-183]: https://github.com/tailscale/tailscale/issues/183
[TS-multi-tailnet]: https://tailscale.com/kb/1509/multiple-tailnets
[TS-switch]: https://github.com/tailscale/tailscale/blob/v1.104.1/ipn/ipnlocal/local.go#L8399-L8436
[TS-tsnet-doc]: https://tailscale.com/docs/features/tsnet
[TS-tsnet-pkg]: https://pkg.go.dev/tailscale.com/tsnet
[TS-tsnet-src]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L10-L20
[TS-tsnet-port]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L312-L315
[TS-tsnet-dial]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L371-L381
[TS-tsnet-loopback]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L460-L537
[TS-tsnet-engine]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L879-L930
[TS-tsnet-svc]: https://github.com/tailscale/tailscale/blob/v1.104.1/tsnet/tsnet.go#L1787-L1791
[TS-tsdial-resolve]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/tsdial/tsdial.go#L423-L436
[TS-tsdial-userdial]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/tsdial/tsdial.go#L588-L625
[TS-magicsock]: https://github.com/tailscale/tailscale/blob/v1.104.1/wgengine/magicsock/magicsock.go#L3747
[TS-derphttp]: https://github.com/tailscale/tailscale/blob/v1.104.1/derp/derphttp/derphttp_client.go#L613
[TS-netns-darwin]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/netns/netns_darwin.go#L39-L170
[TS-defaultroute-darwin]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/netmon/defaultroute_darwin.go
[TS-bsd-defaultroute]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/netmon/interfaces_bsd.go#L30-L63
[TS-darwin-delegate]: https://github.com/tailscale/tailscale/blob/v1.104.1/net/netmon/interfaces_darwin.go#L40-L100
[TS-syspolicy]: https://github.com/tailscale/tailscale/tree/v1.104.1/util/syspolicy/source
[TS-tailscaled-src]: https://github.com/tailscale/tailscale/blob/v1.104.1/cmd/tailscaled/tailscaled.go#L216-L223
[TS-tailscaled-kb]: https://tailscale.com/kb/1278/tailscaled
[TS-userspace]: https://tailscale.com/kb/1112/userspace-networking
[TS-macos-variants]: https://tailscale.com/kb/1065/macos-variants
[TS-other-vpns]: https://tailscale.com/docs/reference/faq/other-vpns
[TS-sharing]: https://tailscale.com/kb/1084/sharing
[TS-services]: https://tailscale.com/kb/1552/tailscale-services
[TS-app-connectors]: https://tailscale.com/kb/1281/app-connectors
[TS-funnel]: https://tailscale.com/docs/features/tailscale-funnel
[TS-derp]: https://tailscale.com/kb/1232/derp-servers
[TS-pricing]: https://tailscale.com/pricing
[TS-pricing-faq]: https://tailscale.com/kb/1251/pricing-faq
[TS-terms]: https://tailscale.com/legal/terms
[GoProxy-ts]: https://proxy.golang.org/tailscale.com/@latest

Headscale (v0.29.5 unless marked `main`):

[HS-releases]: https://github.com/juanfont/headscale/releases
[HS-features]: https://github.com/juanfont/headscale/blob/v0.29.5/docs/about/features.md
[HS-faq]: https://github.com/juanfont/headscale/blob/v0.29.5/docs/about/faq.md
[HS-clients]: https://github.com/juanfont/headscale/blob/v0.29.5/docs/about/clients.md
[HS-apple]: https://github.com/juanfont/headscale/blob/v0.29.5/docs/usage/connect/apple.md
[HS-registration]: https://github.com/juanfont/headscale/blob/v0.29.5/docs/ref/registration.md
[HS-preauthkeys]: https://github.com/juanfont/headscale/blob/v0.29.5/cmd/headscale/cli/preauthkeys.go#L25-L33
[HS-node]: https://github.com/juanfont/headscale/blob/v0.29.5/hscontrol/types/node.go#L86
[HS-users]: https://github.com/juanfont/headscale/blob/v0.29.5/hscontrol/types/users.go#L186-L188
[HS-rel-0290]: https://github.com/juanfont/headscale/releases/tag/v0.29.0
[HS-2845]: https://github.com/juanfont/headscale/issues/2845
[HS-changelog-main]: https://github.com/juanfont/headscale/blob/691ef56329854e952efed5e3c81ea600f46a58b5/CHANGELOG.md
[HS-nix]: https://github.com/juanfont/headscale/blob/691ef56329854e952efed5e3c81ea600f46a58b5/nix/README.md

Other overlays:

[NB-embed]: https://github.com/netbirdio/netbird/blob/v0.81.3/client/embed/embed.go
[NB-status]: https://github.com/netbirdio/netbird/blob/v0.81.3/client/internal/peer/status.go
[NB-license]: https://github.com/netbirdio/netbird/blob/v0.81.3/LICENSE
[NB-readme]: https://github.com/netbirdio/netbird/blob/v0.81.3/README.md
[Neb-service]: https://github.com/slackhq/nebula/blob/v1.11.2/service/service.go
[Neb-control]: https://github.com/slackhq/nebula/blob/v1.11.2/control.go#L237-L276
[Neb-config]: https://github.com/slackhq/nebula/blob/v1.11.2/examples/config.yml#L234-L246
[Neb-readme]: https://github.com/slackhq/nebula/blob/v1.11.2/README.md
[ZT-libzt]: https://github.com/zerotier/libzt/blob/main/README.md
[ZT-one]: https://github.com/zerotier/ZeroTierOne/blob/dev/README.md
[NM-readme]: https://github.com/gravitl/netmaker/blob/master/README.md
[Inn-readme]: https://github.com/tonarino/innernet/blob/main/README.md
[WG-netstack]: https://pkg.go.dev/golang.zx2c4.com/wireguard/tun/netstack
[GoProxy-wg]: https://proxy.golang.org/golang.zx2c4.com/wireguard/@latest

Web platform:

[RFC6265-8.5]: https://www.rfc-editor.org/rfc/rfc6265#section-8.5
[Chromium-proxy]: https://chromium.googlesource.com/chromium/src/+/HEAD/net/docs/proxy.md

Current repository sources (relative links refer to the inspected checkout):

[W-go]: ../../go.mod
[W-fabric]: ../../fabric/fabric.go
[W-tsnet]: ../../fabric/tsnet/tsnet.go
[Synthesis-§6]: 2026-08-06-deep-research-synthesis.md
