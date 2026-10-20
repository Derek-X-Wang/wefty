#!/usr/bin/env python3
"""PROTOTYPE check driver (wefty #1028). Runs as root on a Linux runner.

Drives the proto-harness (real BuildRuntimeSpec profile) + a rootful containerd,
writes /wefty/control/seats.json from the host side, probes Seat endpoints from
the host through the task's network namespace, and records a receipt.
"""
import json, os, re, shutil, subprocess, sys, time

SOCK = os.environ.get("PROTO_CONTAINERD_SOCK", "/run/wefty-containerd/containerd.sock")
OUT = os.environ.get("PROTO_OUT", "/tmp/proto-out")
ROOT = "/var/lib/proto-oci"
CONTROL = ROOT + "/control/seats.json"
SERVICE = ROOT + "/service"
CG = "/sys/fs/cgroup/proto-seats"
HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = HERE + "/rfb-probe.py"
REF = "docker.io/library/proto-seats:1"
PAGE = "file:///opt/proto/page.html"
COLORS = {1: "ff0000", 2: "0000aa", 5: "aa00aa"}

res = {"checks": [], "timing": {}, "memory": {}, "env": {}, "notes": []}
state = {"pid": 0, "gen": 0, "xid": 0}


def log(*a):
    print(*a, flush=True)


def run(cmd, timeout=180, check=True):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError("%s -> rc=%d\n%s\n%s" % (cmd, p.returncode, p.stdout[-2000:], p.stderr[-2000:]))
    return p


def rec(cid, name, verdict, evidence):
    res["checks"].append({"id": cid, "name": name, "verdict": verdict, "evidence": evidence})
    log("CHECK %s %s: %s | %s" % (cid, verdict, name, str(evidence)[:300]))


def cexec(argv, user=None, timeout=90):
    state["xid"] += 1
    cmd = ["ctr", "-a", SOCK, "-n", "wefty", "task", "exec", "--exec-id", "x%d" % state["xid"]]
    if user:
        cmd += ["--user", user]
    p = run(cmd + ["proto-seats"] + argv, timeout=timeout, check=False)
    return p.returncode, (p.stdout + p.stderr).strip()


def probe(mode, port, *args, timeout=120):
    return run(["nsenter", "-t", str(state["pid"]), "-n", "python3", PROBE, mode, str(port), *map(str, args)],
               timeout=timeout, check=False)


def ports(n):
    return 7000 + n * 10 + 1, 7000 + n * 10 + 2


def seat_spec(n, heavy=False, numeric=False):
    v, c = ports(n)
    s = {"id": n, "view_port": v, "control_port": c,
         "url": "%s?c=%s&heavy=%d" % (PAGE, COLORS.get(n, "808080"), 1 if heavy else 0)}
    if numeric:
        s.update({"uid": 60000 + n, "gid": 60000 + n})
    else:
        s["user"] = "seat%d" % n
    return s


def write_seats(seats):
    state["gen"] += 1
    tmp = CONTROL + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"generation": state["gen"], "seats": seats}, f)
    os.rename(tmp, CONTROL)            # atomic replace, like the helper
    return time.time()


def wait_banner(port, t0, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        p = probe("banner", port, timeout=15)
        if p.returncode == 0 and "RFB" in p.stdout:
            return round(time.time() - t0, 2)
        time.sleep(0.2)
    return None


def wait_frame(port, color, t0, timeout=120):
    p = probe("frame", port, "--expect", color, "--timeout", timeout, timeout=timeout + 30)
    if p.returncode != 0:
        return None
    return round(time.time() - t0, 2)


def add_seats(label, seats, specs_for_timing):
    """Write seats.json, time banner (view+control) and first browser frame for each listed seat."""
    t0 = write_seats(seats)
    out = {}
    for n, color in specs_for_timing:
        v, c = ports(n)
        out["seat%d" % n] = {"banner_view_s": wait_banner(v, t0), "banner_control_s": wait_banner(c, t0),
                             "first_browser_frame_s": wait_frame(v, color, t0)}
    res["timing"][label] = out
    log("TIMING", label, out)
    return out


def status_file():
    rc, out = cexec(["cat", "/run/seat-status.json"])
    try:
        return json.loads(out)
    except ValueError:
        return {"raw": out}


def read_cg(name):
    try:
        with open(CG + "/" + name) as f:
            return f.read()
    except OSError as e:
        return "ERR %s" % e


def mem(label, settle=0):
    time.sleep(settle)
    os.makedirs(OUT + "/mem", exist_ok=True)
    snap = {n: read_cg(n) for n in ("memory.current", "memory.peak", "memory.events", "memory.stat", "memory.max", "memory.swap.max", "pids.current")}
    for n, t in snap.items():
        with open("%s/mem/%s.%s" % (OUT, label, n), "w") as f:
            f.write(t)
    stat = {}
    for line in snap["memory.stat"].splitlines():
        k, _, v = line.partition(" ")
        if v.strip().isdigit():
            stat[k] = int(v)
    mb = lambda x: round(x / 1048576, 1)
    cur = int(snap["memory.current"]) if snap["memory.current"].strip().isdigit() else 0
    peak = int(snap["memory.peak"]) if snap["memory.peak"].strip().isdigit() else 0
    anon, shmem, filec = stat.get("anon", 0), stat.get("shmem", 0), stat.get("file", 0)
    row = {"current_mib": mb(cur), "peak_mib": mb(peak), "anon_mib": mb(anon), "shmem_mib": mb(shmem),
           "program_mib_anon_plus_shmem": mb(anon + shmem), "file_cache_mib_excl_shmem": mb(filec - shmem),
           "kernel_mib": mb(stat.get("kernel", 0)), "slab_mib": mb(stat.get("slab", 0)),
           "kernel_stack_mib": mb(stat.get("kernel_stack", 0)), "events": snap["memory.events"].split(),
           "pids": snap["pids.current"].strip()}
    res["memory"][label] = row
    log("MEM", label, row)


def shot(label, n):
    os.makedirs(OUT + "/shots", exist_ok=True)
    path = "%s/shots/%s_seat%d.png" % (OUT, label, n)
    p = probe("shot", ports(n)[0], path, timeout=60)
    log("SHOT", path, p.stdout.strip() or p.stderr.strip()[-200:])
    return p.stdout.strip()


def strip_of(n):
    p = probe("pixel", ports(n)[0], timeout=60)
    try:
        return json.loads(p.stdout)
    except ValueError:
        return {"strip": None, "err": p.stderr[-200:]}


def click(port):
    return probe("click", port, timeout=60)


def poll_strip(n, want, secs=8):
    end = time.time() + secs
    last = None
    while time.time() < end:
        last = strip_of(n)
        if last.get("strip") == want:
            return True, last
        time.sleep(0.3)
    return False, last


def up():
    t = time.time()
    p = run(["proto-harness", "up", REF], timeout=300)
    info = json.loads(p.stdout.strip().splitlines()[-1])
    state["pid"] = info["pid"]
    info["wall_s"] = round(time.time() - t, 2)
    res["timing"].setdefault("harness_up", []).append(info)
    for _ in range(100):
        st = status_file()
        if "applied_generation" in st:
            break
        time.sleep(0.3)
    return info


def procs_of_uid(uid):
    rc, out = cexec(["sh", "-c", "ps -eo uid,pid,args | awk '$1==%d'" % uid])
    return [l for l in out.splitlines() if l.strip()]


def main():
    os.makedirs(OUT, exist_ok=True)
    res["env"] = {"arch": run(["uname", "-m"]).stdout.strip(), "kernel": run(["uname", "-r"]).stdout.strip(),
                  "containerd": run(["containerd", "--version"]).stdout.strip(),
                  "runc": run(["runc", "--version"]).stdout.splitlines()[0],
                  "cgroup_fs": run(["stat", "-fc", "%T", "/sys/fs/cgroup"]).stdout.strip(),
                  "nproc": run(["nproc"]).stdout.strip(), "mem_total_mib": int(re.search(r"MemTotal:\s+(\d+)", open("/proc/meminfo").read()).group(1)) // 1024}
    shutil.rmtree(ROOT + "/control", ignore_errors=True)
    os.makedirs(ROOT + "/control", exist_ok=True)
    write_seats([])
    info = up()
    log("UP", info)
    shutil.copy(ROOT + "/spec.json", OUT + "/spec.json")
    mem("0_supervisor_only", settle=10)

    # ---- (a) blank page, Seat 1 then Seat 2
    t1 = add_seats("a_blank_add_seat1", [seat_spec(1)], [(1, COLORS[1])])
    time.sleep(20)
    shot("1seat", 1)
    mem("1_seat_idle", settle=0)
    t2 = add_seats("a_blank_add_seat2", [seat_spec(1), seat_spec(2)], [(2, COLORS[2])])
    time.sleep(20)
    shot("2seats", 1)
    shot("2seats", 2)
    mem("2_seats_idle", settle=0)
    ok_a = all(v["banner_view_s"] and v["banner_control_s"] and v["first_browser_frame_s"] for t in (t1, t2) for v in t.values())
    rec("a", "add Seat 1 then Seat 2 (blank); RFB banner and first browser frame", "PASS" if ok_a else "FAIL",
        {"seat1": t1, "seat2": t2, "status": status_file()})

    # ---- (b) distinct framebuffers, input isolation, view-only
    s1, s2 = strip_of(1), strip_of(2)
    distinct = s1.get("strip") == COLORS[1] and s2.get("strip") == COLORS[2] and s1.get("digest") != s2.get("digest")
    click(ports(2)[0])                       # click on Seat 2 VIEW port: must be ignored
    time.sleep(1.5)
    view_ignored = strip_of(2).get("strip") == COLORS[2]
    click(ports(1)[1])                       # click on Seat 1 CONTROL port
    ok1, after1 = poll_strip(1, "00ff00")
    still2 = strip_of(2).get("strip") == COLORS[2]
    rec("b", "distinct framebuffers; input on Seat 1 control changes only Seat 1; view port ignores input",
        "PASS" if (distinct and view_ignored and ok1 and still2) else "FAIL",
        {"seat1_strip": s1, "seat2_strip": s2, "view_port_click_ignored": view_ignored,
         "seat1_after_control_click": after1, "seat2_unchanged": still2})

    # ---- (c) command as the Seat user with its screen/HOME; background programs outlive it
    c_ev = {}
    for n in (1, 2):
        uid = 2000 + n
        home = "/wefty/service/seats/%d/home" % n
        script = ("echo uid=$(id -u) home=$HOME display=$DISPLAY; ls -a $HOME | head -5 | tr '\\n' ' '; echo; "
                  "xdpyinfo | grep -m1 dimensions; echo from-seat%d > /wefty/service/shared/from-seat%d; "
                  "nohup sleep 600 >/dev/null 2>&1 & "
                  "(setsid sh -c 'sleep 601 & sleep 602' >/dev/null 2>&1 &) ; echo started" % (n, n))
        rc, out = cexec(["env", "HOME=" + home, "DISPLAY=:%d" % n,
                         "XAUTHORITY=/wefty/service/seats/%d/.Xauthority" % n, "XDG_RUNTIME_DIR=/wefty/service/seats/%d/run" % n,
                         "sh", "-c", script], user="%d:%d" % (uid, uid))
        time.sleep(1)
        left = procs_of_uid(uid)
        sleeps = [l for l in left if re.search(r"sleep 60[012]", l)]
        c_ev["seat%d" % n] = {"rc": rc, "output": out, "background_sleeps_alive_after_command": len(sleeps)}
    ok_c = all(v["rc"] == 0 and v["background_sleeps_alive_after_command"] >= 3 and "dimensions" in v["output"] and ("uid=%d" % (2000 + int(k[-1]))) in v["output"]
               for k, v in c_ev.items())
    rec("c", "exec as Seat user sees own HOME and screen; plain and double-forked background programs outlive the command",
        "PASS" if ok_c else "FAIL", c_ev)

    # ---- (d) Seat 2 reaching Seat 1 (OBSERVED)
    d_script = r'''
echo "[draw-on-seat1-display, own cookie]"; DISPLAY=:1 XAUTHORITY=/wefty/service/seats/2/.Xauthority xdpyinfo 2>&1 | head -2
echo "[draw-on-seat1-display, no cookie]"; DISPLAY=:1 XAUTHORITY=/nonexistent xdpyinfo 2>&1 | head -2
echo "[read seat1 .Xauthority]"; cat /wefty/service/seats/1/.Xauthority >/dev/null 2>&1 && echo READ || echo DENIED
echo "[list seat1 folder]"; ls /wefty/service/seats/1 2>&1 | head -3
echo "[list seat1 home]"; ls /wefty/service/seats/1/home 2>&1 | head -3
echo "[dial seat1 control websocket port 7012]"; python3 -c "import socket;socket.create_connection(('127.0.0.1',7012),2);print('CONNECTED')" 2>&1 | tail -1
echo "[dial seat1 x11vnc control backend 17012]"; python3 -c "import socket;s=socket.create_connection(('127.0.0.1',17012),2);print('CONNECTED', s.recv(12))" 2>&1 | tail -1
echo "[X sockets visible]"; ls /tmp/.X11-unix
echo "[see seat1 processes]"; ps -u 2001 -o pid,args | head -3
echo "[signal seat1 process]"; kill -0 $(pgrep -u 2001 -o Xvfb) 2>&1 && echo ALLOWED || echo DENIED
echo "[shared folder]"; ls -l /wefty/service/shared
'''
    rc, out = cexec(["sh", "-c", d_script], user="2002:2002")
    rec("d", "Seat 2 process reaching Seat 1 (X display, VNC control port, files, signals)", "OBSERVED", out)

    # ---- (f) remove Seat 2
    uid2_before = procs_of_uid(2002)
    t0 = write_seats([seat_spec(1)])
    gone = None
    for _ in range(300):
        rc, out = cexec(["sh", "-c", "test -e /wefty/service/seats/2 && echo present || echo gone"])
        if out.strip() == "gone":
            gone = round(time.time() - t0, 2)
            break
        time.sleep(0.2)
    left = procs_of_uid(2002)
    rc, ss = cexec(["sh", "-c", "ss -ltnxp | grep -E ':(7021|7022|17021|17022)\\b|X11-unix/X2\\b' || echo none"])
    rc, ex = cexec(["sh", "-c", "ls /tmp/.X11-unix /tmp/.X2-lock 2>&1; ls -A /tmp /dev/shm | grep -c . ; find /tmp /var/tmp /dev/shm /run -xdev -user 2002 2>/dev/null | wc -l"])
    rc, shared = cexec(["sh", "-c", "cat /wefty/service/shared/from-seat2"])
    s1_banner = probe("banner", ports(1)[0], timeout=15)
    s1_strip = strip_of(1)
    shot("after_seat2_removed_blank", 1)
    mem("after_seat2_removed_blank", settle=5)
    t_re = add_seats("f_blank_readd_seat2", [seat_spec(1), seat_spec(2)], [(2, COLORS[2])])
    ok_f = (gone is not None and not left and "none" in ss and shared == "from-seat2" and s1_banner.returncode == 0
            and all(t_re["seat2"].values()))
    rec("f", "remove Seat 2: no processes, no sockets, folder gone, shared file survives, Seat 1 answers; re-add works",
        "PASS" if ok_f else "FAIL",
        {"procs_before": len(uid2_before), "procs_after": left, "folder_gone_s": gone, "sockets_after": ss,
         "scratch_leftovers": ex, "shared_file": shared, "seat1_banner": s1_banner.stdout.strip(),
         "seat1_strip": s1_strip, "readd_timing": t_re})

    # ---- heavy phase: both Seats on the heavy page, then viewer, then remove Seat 2
    write_seats([])
    for _ in range(300):
        rc, out = cexec(["sh", "-c", "ls /wefty/service/seats | wc -l"])
        if out.strip() == "0":
            break
        time.sleep(0.2)
    th1 = add_seats("heavy_add_seat1", [seat_spec(1, True)], [(1, COLORS[1])])
    th2 = add_seats("heavy_add_seat2", [seat_spec(1, True), seat_spec(2, True)], [(2, COLORS[2])])
    time.sleep(20)
    shot("2seats_heavy", 1)
    shot("2seats_heavy", 2)
    mem("2_seats_heavy", settle=0)
    viewers = [subprocess.Popen(["nsenter", "-t", str(state["pid"]), "-n", "python3", PROBE, "hold", str(ports(n)[0]), "30"],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for n in (1, 2)]
    time.sleep(15)
    mem("2_seats_heavy_viewer_connected", settle=0)
    for v in viewers:
        try:
            v.wait(timeout=45)
        except subprocess.TimeoutExpired:
            v.kill()
    res["notes"].append("viewer clients (hold) outputs: " + " | ".join(v.stdout.read().strip() for v in viewers if v.stdout))
    write_seats([seat_spec(1, True)])
    time.sleep(15)
    shot("after_seat2_removed_heavy", 1)
    mem("after_seat2_removed_heavy", settle=0)

    # ---- (e) numeric uid with no passwd entry
    t0 = write_seats([seat_spec(1, True), seat_spec(5, numeric=True)])
    banner5 = wait_banner(ports(5)[0], t0, 90)
    frame5 = wait_frame(ports(5)[0], COLORS[5], t0, 60) if banner5 else None
    rc, ps5 = cexec(["sh", "-c", "ps -u 60005 -o pid,args | cut -c1-110 | head -20; echo ---; getent passwd 60005 || echo 'no passwd entry'; echo ---; tail -n 15 /run/seat-5.log"])
    shot("numeric_uid", 5)
    rec("e", "numeric-uid Seat (no passwd entry): desktop and Chromium start", "PASS" if frame5 else "FAIL",
        {"banner_s": banner5, "first_browser_frame_s": frame5, "ps_and_log": ps5})
    write_seats([seat_spec(1, True)])
    time.sleep(5)

    # ---- (g) Computer restart ends every Seat, shared folder survives
    before = len(procs_of_uid(2001))
    write_seats([seat_spec(1, True)])
    run(["proto-harness", "down"], timeout=120)
    t = time.time()
    rc_ls = run(["ctr", "-a", SOCK, "-n", "wefty", "task", "ls"], check=False).stdout
    write_seats([])                       # a restart gets a fresh control dir
    info = up()
    rc, after = cexec(["sh", "-c", "ps -eo uid | awk '$1>=2000' | wc -l; ls -A /wefty/service/seats | wc -l; ls /wefty/service/shared"])
    st = status_file()
    lines = after.split("\n")
    ok_g = (before > 0 and "proto-seats" not in rc_ls.replace("TASK", "") and lines[0].strip() == "0" and lines[1].strip() == "0"
            and "from-seat1" in after and "from-seat2" in after and not st.get("seats"))
    rec("g", "Computer restart (stop+start task) ends every Seat; shared folder survives", "PASS" if ok_g else "FAIL",
        {"seat1_procs_before": before, "task_list_after_down": rc_ls.strip(), "after_restart": after, "status": st,
         "restart_wall_s": round(time.time() - t, 2)})

    with open(OUT + "/receipt.json", "w") as f:
        json.dump(res, f, indent=2)
    run(["cp", "/var/log/proto-container.log", OUT + "/container.log"], check=False)
    log("DONE")


if __name__ == "__main__":
    try:
        main()
    finally:
        os.makedirs(OUT, exist_ok=True)
        with open(OUT + "/receipt.json", "w") as f:
            json.dump(res, f, indent=2)
        # diagnostics even on failure
        for name, cmd in (("supervisor-status.txt", ["cat", "/run/seat-status.json"]),
                          ("seat-logs.txt", ["sh", "-c", "tail -n 40 /run/seat-*.log"])):
            try:
                rc, out = cexec(cmd)
                open(OUT + "/" + name, "w").write(out)
            except Exception as e:      # noqa
                open(OUT + "/" + name, "w").write(repr(e))
