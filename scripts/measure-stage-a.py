#!/usr/bin/env python3
"""Tagged Stage A observations. Never installs or removes an installation.

Live commands require WEFTY_ATTENDED_STAGE_A=1. SQLite observations are one
read-only transaction. Command success is supporting evidence, never a row pass.
"""
import argparse
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time


def snapshot(database, monotonic_ns):
    if database.is_symlink():
        raise ValueError('refusing redirected proof database')
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        seats = [dict(r) for r in db.execute("""SELECT a.seat_id,
            COALESCE(NULLIF(a.lifecycle_state,''),a.state) AS state,
            CASE WHEN a.lifecycle_state='' THEN a.reason ELSE a.lifecycle_reason END AS reason,
            COALESCE(i.computer_id,'') AS computer_id, COALESCE(i.place,0) AS place,
            a.source, a.deadline_ns FROM seat_approvals a
            LEFT JOIN computer_seat_identities i ON i.seat_id=a.seat_id ORDER BY a.seat_id""")]
        computers = [dict(r) for r in db.execute('''SELECT c.computer_id,c.name,c.desired_state,
            c.removal_outcome,c.current_job_id,j.current_attempt_id,s.published_attempt_id,s.display_endpoint
            FROM computers c LEFT JOIN jobs j ON j.job_id=c.current_job_id
            LEFT JOIN service_jobs s ON s.job_id=c.current_job_id ORDER BY c.computer_id''')]
        attempts = [dict(r) for r in db.execute('''SELECT attempt_id,job_id,node_id,boot_session_id,
            state,lease_expires_ns FROM attempts ORDER BY job_id,created_ns,attempt_id''')]
        observations = [dict(r) for r in db.execute('''SELECT seat_id,attempt_id,state,reason,observed_ns,
            cleanup_complete FROM computer_seat_observations ORDER BY seat_id,attempt_id''')]
        events = [json.loads(r[0]) for r in db.execute('SELECT event_json FROM seat_log_events ORDER BY ordinal')]
        nodes = [dict(r) for r in db.execute('SELECT node_id,boot_session_id,state,capability_reason_code FROM nodes ORDER BY node_id')]
    return dict(monotonic_ns=monotonic_ns, observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                seats=seats, computers=computers, events=events, nodes=nodes,
                attempts=attempts, seat_observations=observations)


def watch(database, facts, output, seconds, guard, clock=time.monotonic, sleep=time.sleep):
    end = clock() + seconds
    while clock() < end:
        guard()
        value = snapshot(database, int(clock() * 1e9))
        # Retain actual recorded facts, including their source timestamp. Missing
        # or stale facts never become a guessed stopped/running state.
        value['host_facts'] = json.loads(facts.read_text()) if facts.exists() else None
        output.write(json.dumps(value) + '\n')
        output.flush()
        sleep(1)


def event_time(event):
    value = re.sub(r'\.(\d{6})\d+', r'.\1', event['occurred_at'])
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def seat_timing(value, seat):
    events = [e for e in value['events'] if e['seat_id'] == seat]
    positions = []
    for kind in ['requested', 'approved', 'started']:
        matches = [i for i, e in enumerate(events) if e['kind'] == kind]
        if len(matches) != 1:
            raise ValueError('need exactly one requested, approved and started event')
        positions.append(matches[0])
    if positions != sorted(positions):
        raise ValueError('lifecycle events are out of order')
    times = [event_time(events[i]) for i in positions]
    if times != sorted(times):
        raise ValueError('clock changed: wall timestamps cannot measure this interval')
    return times[1] - times[0], times[2] - times[1]


def warning_before_end(value, seat):
    events = [e for e in value['events'] if e['seat_id'] == seat]
    warnings = [i for i, e in enumerate(events) if e['kind'] == 'ends_soon']
    ends = [i for i, e in enumerate(events) if e['kind'] == 'ended']
    return bool(len(warnings) == 1 and len(ends) == 1 and warnings[0] < ends[0]
                and events[ends[0]].get('reason'))


def room_refusal(before, after, incumbents, refused):
    old = {s['seat_id']: s for s in before['seats']}
    new = {s['seat_id']: s for s in after['seats']}
    reject = new.get(refused, {})
    return bool(len(incumbents) == 3 and len(set(incumbents)) == 3
                and reject.get('state') == 'refused'
                and reject.get('reason') == 'out of room, try again later'
                and not reject.get('computer_id')
                and {c['computer_id'] for c in before['computers']} == {c['computer_id'] for c in after['computers']}
                and all(i in old and i in new and old[i]['state'] == new[i]['state'] == 'live'
                        and old[i]['computer_id'] == new[i]['computer_id']
                        and old[i]['place'] == new[i]['place'] for i in incumbents))


def removed(value, computer):
    matches = [c for c in value['computers'] if c['computer_id'] == computer]
    return bool(len(matches) == 1 and matches[0]['desired_state'] == 'removed'
                and matches[0]['removal_outcome'] == 'removed_verified')


def matrix_rows(repo):
    return [line.split('|')[1].strip() for line in
            (repo / 'docs/acceptance/stage-a-matrix.md').read_text().splitlines()
            if re.match(r'^\| [A-Z]+-\d+ \|', line)]


def validate_matrix(repo):
    spec = (repo / 'docs/specs/2026-10-21-computer-request-spec.md').read_text()
    section = spec.split('## 11. Acceptance', 1)[1].split('## 12.', 1)[0]
    expected = []
    stage_b = False
    for line in section.splitlines():
        if line.startswith('### '):
            stage_b = 'Stage B' in line
        match = re.match(r'^\| ([A-Z]+-\d+) \|', line)
        if match and not stage_b:
            expected.append(match[1])
    actual = matrix_rows(repo)
    if sorted(actual) != sorted(expected) or len(set(actual)) != len(actual):
        raise ValueError('matrix must contain every Stage A row exactly once')
    content = (repo / 'docs/acceptance/stage-a-matrix.md').read_text()
    for path, name in re.findall(r'`([^` ]+\.go):(Test\w+)`', content):
        if not re.search(r'^func ' + re.escape(name) + r'\(', (repo / path).read_text(), re.M):
            raise ValueError('missing test: ' + path + ':' + name)


def report(repo, entries):
    print('| Check | Verdict | Evidence / limit |')
    print('|---|---|---|')
    for row in matrix_rows(repo):
        values = [e for e in entries if e['row'] == row]
        if not values:
            print(f'| {row} | NOT-RUN | No attended observation in this run. |')
        else:
            detail = '; '.join(e['detail'].replace('|', '\\|').replace('\n', ' ') for e in values)
            print(f'| {row} | PARTIAL | {detail}; owner review required. |')


def validate_command(command):
    if not command or command[0] not in ['seat', 'node', 'doctor', 'status', 'logs', 'exec', 'cp']:
        raise ValueError('capture accepts proof observations and Seat operations only')
    if command[0] == 'node' and command[1:] not in [['doctor'], ['oci', 'off'], ['oci', 'on-demand'], ['oci', 'always-on']]:
        raise ValueError('node capture permits doctor and the three OCI positions only')
    if command[0] == 'exec' and (len(command) < 5 or command[1] != '--seat' or '--' not in command[3:]):
        raise ValueError('exec capture requires --seat SEAT_ID and -- COMMAND')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['snapshot', 'watch', 'capture', 'report', 'check'])
    parser.add_argument('--row')
    parser.add_argument('--name', default='observation')
    parser.add_argument('--before', type=Path)
    parser.add_argument('--after', type=Path)
    parser.add_argument('--seat')
    parser.add_argument('--computer')
    parser.add_argument('--incumbents', nargs=3)
    parser.add_argument('--kind', choices=['timing', 'room', 'removed', 'sleep-log'])
    parser.add_argument('--seconds', type=int, default=120)
    # Split the captured command before parsing observation options.
    argv = sys.argv[1:]
    command = []
    if '--' in argv:
        at = argv.index('--')
        argv, command = argv[:at], argv[at + 1:]
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parent.parent
    validate_matrix(repo)
    if os.environ.get('WEFTY_ATTENDED_STAGE_A') != '1':
        parser.error('requires WEFTY_ATTENDED_STAGE_A=1 and the owner present')
    spec = importlib.util.spec_from_file_location('per_user_proof', Path(__file__).with_name('measure-per-user-proof.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    proof = module.Proof(argparse.Namespace(instance='wefty-proof'))
    receipts = proof.evidence / 'stage-a'
    results = receipts / 'observations.jsonl'
    if args.action == 'report':
        entries = [json.loads(line) for line in results.read_text().splitlines()] if results.exists() else []
        report(repo, entries)
        return
    proof.guard(force=True)
    if args.row not in matrix_rows(repo) or not re.fullmatch(r'[a-zA-Z0-9_-]+', args.name):
        parser.error('valid --row and simple unique --name required')
    receipts.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination = receipts / (args.name + ('.jsonl' if args.action == 'watch' else '.json'))
    if destination.exists():
        parser.error('receipt exists: choose a new --name; never overwrite evidence')
    if args.action == 'watch':
        if not 1 <= args.seconds <= 7200:
            parser.error('watch requires 1..7200 seconds')
        with destination.open('x') as output:
            watch(proof.data / 'l1.sqlite', proof.data / 'oci-facts.json', output,
                  args.seconds, lambda: proof.guard(force=True))
        detail = 'L1/recorded host-facts trace: ' + destination.name + '; inspect source observation times for staleness'
    elif args.action == 'snapshot':
        value = snapshot(proof.data / 'l1.sqlite', time.monotonic_ns())
        detail = 'read-only L1 snapshot: ' + destination.name
    elif args.action == 'check':
        if not args.after or not args.kind:
            parser.error('check requires --after and --kind')
        after = json.loads(args.after.read_text())
        if args.kind == 'timing':
            outcome = seat_timing(after, args.seat)
            detail = f'request-to-approval={outcome[0]:.6f}s; approval-to-started={outcome[1]:.6f}s (L1 log times, not browser readiness)'
        else:
            if args.kind == 'room':
                if not args.before or not args.incumbents:
                    parser.error('room requires --before and --incumbents')
                outcome = room_refusal(json.loads(args.before.read_text()), after, args.incumbents, args.seat)
            elif args.kind == 'removed':
                outcome = removed(after, args.computer)
            else:
                outcome = warning_before_end(after, args.seat)
            detail = args.kind + (' supporting check passed' if outcome else ' CHECK FAILED; keep row unverified')
        value = dict(kind=args.kind, outcome=outcome, before=str(args.before), after=str(args.after))
    else:
        try:
            validate_command(command)
        except ValueError as error:
            parser.error(str(error))
        # Pin every selector, including credential state, to the disposable install.
        proof.env = {k: v for k, v in proof.env.items() if not k.startswith('WEFTY_')}
        proof.env.update(WEFTY_NODE_CONFIG=str(proof.root / 'config/node.json'),
                         XDG_STATE_HOME=str(proof.data / 'stage-a-state'), WEFTY_SEAT_SCREEN_TIMING='1')
        # A Seat command outside Git needs no working-copy sync. The public
        # browser kit is explicitly copied; do not accidentally copy this repo.
        if command[0] == 'exec':
            os.chdir(proof.root / 'assets')
        begin = time.monotonic_ns()
        begin_unix = time.time_ns()
        result, elapsed = proof.run([str(proof.root / 'source-bin/wefty'), '--profile', str(proof.root / 'config/profile.json'), '--json'] + command, timeout=7200, check=False)
        value = dict(command=command, begin_monotonic_ns=begin, end_monotonic_ns=time.monotonic_ns(),
                     begin_unix_ns=begin_unix, end_unix_ns=time.time_ns(),
                     elapsed_seconds=elapsed, exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        detail = f'{destination.name}: exit={result.returncode}; wall={elapsed:.6f}s; inspect raw output'
    if args.action != 'watch':
        with destination.open('x') as output:
            json.dump(value, output, indent=2)
            output.write('\n')
    with results.open('a') as output:
        output.write(json.dumps(dict(row=args.row, detail=detail, receipt=destination.name)) + '\n')
    report(repo, [json.loads(line) for line in results.read_text().splitlines()])
    if args.action == 'capture' and result.returncode:
        sys.exit(result.returncode)
    if args.action == 'check' and outcome is False:
        sys.exit(1)


if __name__ == '__main__':
    main()
