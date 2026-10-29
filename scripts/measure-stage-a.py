#!/usr/bin/env python3
"""Tagged Stage A observations. Never installs or removes an installation.

Live commands require WEFTY_ATTENDED_STAGE_A=1. SQLite observations are one
read-only transaction. Command success is supporting evidence, never a row pass.
"""
import argparse
from contextlib import closing
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time

_proof_spec = importlib.util.spec_from_file_location('per_user_proof', Path(__file__).with_name('measure-per-user-proof.py'))
per_user_proof = importlib.util.module_from_spec(_proof_spec)
_proof_spec.loader.exec_module(per_user_proof)


def install_wrappers(root, script):
    """Prepare absolute selectors. This writes scripts; it runs no proof command."""
    root = root.absolute()
    if root.resolve() != root:
        raise ValueError('refusing redirected proof root')
    for suffix in ['tools', 'data/stage-a-state', 'evidence/stage-a']:
        path = root / suffix
        if path.resolve() != path:
            raise ValueError('refusing redirected proof path')
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    common = f'''#!{sys.executable}
import os, pathlib, sys
root = pathlib.Path({str(root)!r})
script = pathlib.Path({str(script)!r})
binary = root / 'source-bin/wefty'
profile = root / 'config/profile.json'
node = root / 'config/node.json'
state = root / 'data/stage-a-state'
evidence = root / 'evidence/stage-a'
for path in [root, binary, profile, node, state, evidence, script]:
    if not path.exists() or path.resolve() != path:
        sys.exit('missing proof path or redirected proof path: ' + str(path))
env = {{k: v for k, v in os.environ.items() if not k.startswith('WEFTY_')}}
env.update(HOME=str(root.parents[2]), WEFTY_PROFILE=str(profile), WEFTY_NODE_CONFIG=str(node),
           XDG_STATE_HOME=str(state), LIMA_HOME=str(root / 'data/lima'),
           WEFTY_SEAT_SCREEN_TIMING='1', WEFTY_ATTENDED_STAGE_A='1')
'''
    bodies = {
        'w': '''args = sys.argv[1:]
prefix = args[:args.index('--')] if '--' in args else args
selectors = {'profile','l1','l3','fabric','plain-user-id','plain-device-id','plain-fabric-id','fabric-name','node-config'}
if any(a.lstrip('-').split('=')[0] in selectors for a in prefix if a.startswith('-')):
    sys.exit('proof wrapper refuses selector overrides')
# Only JSON switches may precede the command. Do not try to interpret
# arbitrary global flags: an option value could conceal setup or upgrade.
json_flags = {'--json', '-json', '--json=true', '--json=false', '-json=true', '-json=false'}
command_args = [arg for arg in prefix if arg not in json_flags]
if not command_args or command_args[0].startswith('-'):
    sys.exit('proof wrapper refuses leading options other than --json')
if command_args[0] in {'setup', 'upgrade', 'admins'}:
    sys.exit('proof wrapper refuses setup, upgrade and legacy administrator commands')
if command_args[0] == 'admin':
    allowed = (command_args == ['admin', 'policy', 'get'] or
               len(command_args) == 3 and command_args[:2] == ['admin', 'bootstrap'] and
               bool(command_args[2].strip()) and not command_args[2].startswith('-'))
    if not allowed or '--' in args:
        sys.exit('proof wrapper refuses administrator commands except bootstrap and policy get')
if command_args and command_args[0] == 'exec':
    assets = root / 'assets'
    if not assets.is_dir() or assets.resolve() != assets:
        sys.exit('missing proof path: ' + str(assets))
    os.chdir(assets)
os.execve(str(binary), [str(binary), '--profile', str(profile)] + args, env)
''',
        'obs': '''args = sys.argv[1:]
if args and args[0] in ['guard-check', 'guard-watch', 'shutdown']:
    utility = script.with_name('measure-per-user-proof.py')
    os.execve(sys.executable, [sys.executable, str(utility)] + args, env)
os.execve(sys.executable, [sys.executable, str(script)] + args, env)
''',
        'snap': '''os.execve(sys.executable, [sys.executable, str(script), 'snapshot'] + sys.argv[1:], env)
''',
    }
    for name, body in bodies.items():
        target = root / 'tools' / name
        with target.open('x') as output:
            output.write(common + body)
        target.chmod(0o700)


def snapshot(database, monotonic_ns, seat=None, computer=None):
    if database.is_symlink():
        raise ValueError('refusing redirected proof database')
    with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        seats = [dict(r) for r in db.execute("""SELECT a.seat_id,
            COALESCE(NULLIF(a.lifecycle_state,''),a.state) AS state,
            CASE WHEN a.lifecycle_state='' THEN a.reason ELSE a.lifecycle_reason END AS reason,
            COALESCE(i.computer_id,'') AS computer_id, COALESCE(i.place,0) AS place,
            a.source, a.deadline_ns FROM seat_approvals a
            LEFT JOIN computer_seat_identities i ON i.seat_id=a.seat_id
            WHERE (? IS NULL OR a.seat_id=?) ORDER BY a.seat_id""", (seat, seat))]
        computer_filter = computer
        if seat and not computer_filter:
            computer_filter = next((s['computer_id'] for s in seats if s['computer_id']), '')
        computers = [dict(r) for r in db.execute('''SELECT c.computer_id,c.name,c.desired_state,
            c.removal_outcome,c.current_job_id,j.current_attempt_id,s.published_attempt_id,s.display_endpoint
            FROM computers c LEFT JOIN jobs j ON j.job_id=c.current_job_id
            LEFT JOIN service_jobs s ON s.job_id=c.current_job_id
            WHERE (? IS NULL OR c.computer_id=?) ORDER BY c.computer_id''', (computer_filter, computer_filter))]
        attempts = [dict(r) for r in db.execute('''SELECT attempt_id,job_id,node_id,boot_session_id,
            state,lease_expires_ns FROM attempts
            WHERE (? IS NULL OR job_id IN (SELECT current_job_id FROM computers WHERE computer_id=?))
            ORDER BY job_id,created_ns,attempt_id''', (computer_filter, computer_filter))]
        observations = [dict(r) for r in db.execute('''SELECT seat_id,attempt_id,state,reason,observed_ns,
            cleanup_complete FROM computer_seat_observations WHERE (? IS NULL OR seat_id=?)
            ORDER BY seat_id,attempt_id''', (seat, seat))]
        events = [json.loads(r[0]) for r in db.execute('SELECT event_json FROM seat_log_events WHERE (? IS NULL OR seat_id=?) ORDER BY ordinal', (seat, seat))]
        nodes = [dict(r) for r in db.execute('SELECT node_id,boot_session_id,state,capability_reason_code FROM nodes ORDER BY node_id')]
    return dict(monotonic_ns=monotonic_ns, observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                seats=seats, computers=computers, events=events, nodes=nodes,
                attempts=attempts, seat_observations=observations)


def watch(database, facts, output, seconds, guard, clock=time.monotonic, sleep=time.sleep, seat=None, computer=None, stopping=None):
    end = clock() + seconds
    stopping = stopping or database.parent / 'oci-intent.json.stopping'
    while clock() < end:
        guard()
        value = snapshot(database, int(clock() * 1e9), seat=seat, computer=computer)
        # Retain actual recorded facts, including their source timestamp. Missing
        # or stale facts never become a guessed stopped/running state.
        value['host_facts'] = None
        value['host_facts_written_unix_ns'] = None
        if facts.exists():
            # Open once so an atomic replacement cannot pair one generation's
            # JSON with another generation's file timestamp.
            with facts.open() as source:
                value['host_facts'] = json.load(source)
                value['host_facts_written_unix_ns'] = os.fstat(source.fileno()).st_mtime_ns
        value['stopping'] = stopping.exists()
        value['observed_at'] = per_user_proof.timestamp()
        output.write(json.dumps(value) + '\n')
        output.flush()
        sleep(1)


def event_time(event):
    return per_user_proof.seconds(event['occurred_at'])


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


def ordering(trace, computer):
    """Require fresh running evidence at verified removal, then a later stop.

    This proves observation order only. It does not infer the 10-minute idle
    policy from a trace that cannot identify the last remaining demand.
    """
    verified_at = None
    previous = -1
    for sample in trace:
        if sample['monotonic_ns'] <= previous:
            return False
        previous = sample['monotonic_ns']
        facts = sample.get('host_facts') or {}
        host = facts.get('lima', {})
        if host.get('instance') != 'wefty-proof':
            return False
        try:
            observed = per_user_proof.seconds(sample['observed_at'])
            age = observed - sample['host_facts_written_unix_ns'] / 1e9
        except (KeyError, ValueError, TypeError):
            return False
        # The actual writer ticks every 20s. Supervisor ObservedAt is its
        # last state transition, so an unchanged running state can be old.
        if age < 0 or age > 45:
            return False
        if host.get('state') == 'stopped':
            try:
                stopped_at = per_user_proof.seconds(host['observed_at'])
            except (KeyError, TypeError, ValueError):
                return False
            return verified_at is not None and stopped_at > verified_at and removed(sample, computer)
        if host.get('state') != 'running':
            return False
        if sample.get('stopping') and verified_at is None:
            return False
        if removed(sample, computer) and not sample.get('stopping') and verified_at is None:
            verified_at = observed
    return False


def screen_loop(run, output, seconds, clock=time.monotonic, wall_ns=time.time_ns):
    end = clock() + seconds
    while clock() < end:
        begin = wall_ns()
        result, elapsed = run()
        value = dict(begin_unix_ns=begin, end_unix_ns=wall_ns(), elapsed_seconds=elapsed,
                     exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        output.write(json.dumps(value) + '\n')
        output.flush()


def control_timing(value, seat, attempts):
    events = [e for e in value['events'] if e['seat_id'] == seat and e['kind'] == 'control_taken']
    if len(events) != 1:
        return None
    taken = int(event_time(events[0]) * 1e9)
    eligible = []
    for attempt in attempts:
        try:
            body = json.loads(attempt['stderr'])
        except (ValueError, TypeError):
            continue
        if (attempt['begin_unix_ns'] > taken and attempt['exit_code'] == 5
                and body.get('error', {}).get('code') == 'person_driving'):
            eligible.append(attempt)
    if not eligible:
        return None
    first = min(eligible, key=lambda a: a['begin_unix_ns'])
    accepted = [a['begin_unix_ns'] for a in attempts if a['exit_code'] == 0 and a['begin_unix_ns'] < first['begin_unix_ns']]
    if not accepted:
        return None
    elapsed = (first['end_unix_ns'] - taken) / 1e9
    return dict(control_taken_unix_ns=taken, first_refused_begin_unix_ns=first['begin_unix_ns'],
                first_refused_end_unix_ns=first['end_unix_ns'], seconds=elapsed,
                last_accepted_begin_unix_ns=max(accepted) if accepted else None,
                within_one_second=0 <= elapsed <= 1)


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
            verdict = 'FAIL' if any(e.get('verdict') == 'FAIL' for e in values) else 'PARTIAL'
            print(f'| {row} | {verdict} | {detail}; owner review required. |')


def validate_command(command):
    if not command or command[0] not in ['seat', 'node', 'doctor', 'status', 'logs', 'exec', 'cp']:
        raise ValueError('capture accepts proof observations and Seat operations only')
    if command[0] == 'node' and command[1:] not in [['doctor'], ['oci', 'removals'], ['oci', 'off'], ['oci', 'on-demand'], ['oci', 'always-on']]:
        raise ValueError('node capture permits doctor, OCI removals and the three OCI positions only')
    if command[0] == 'exec' and (len(command) < 5 or command[1] != '--seat' or '--' not in command[3:]):
        raise ValueError('exec capture requires --seat SEAT_ID and -- COMMAND')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install-wrappers', 'snapshot', 'watch', 'screen-loop', 'capture', 'report', 'check'])
    parser.add_argument('--row')
    parser.add_argument('--name', default='observation')
    parser.add_argument('--before', type=Path)
    parser.add_argument('--after', type=Path)
    parser.add_argument('--seat')
    parser.add_argument('--computer')
    parser.add_argument('--incumbents', nargs=3)
    parser.add_argument('--kind', choices=['timing', 'room', 'removed', 'sleep-log', 'ordering', 'control'])
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
    if args.action == 'install-wrappers':
        install_wrappers(Path.home() / '.local/share/wefty-proof', Path(__file__).resolve())
        return
    if os.environ.get('WEFTY_ATTENDED_STAGE_A') != '1':
        parser.error('requires WEFTY_ATTENDED_STAGE_A=1 and the owner present')
    proof = per_user_proof.Proof(argparse.Namespace(instance='wefty-proof'))
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
    destination = receipts / (args.name + ('.jsonl' if args.action in ['watch', 'screen-loop'] else '.json'))
    if destination.exists():
        parser.error('receipt exists: choose a new --name; never overwrite evidence')
    if args.action == 'screen-loop':
        if not args.seat or not 1 <= args.seconds <= 60:
            parser.error('screen-loop requires --seat and 1..60 seconds')
        with destination.open('x') as output:
            screen_loop(lambda: proof.run([str(proof.root / 'tools/w'), 'seat', 'key', args.seat, 'Tab', '--json'],
                                          timeout=10, check=False), output, args.seconds)
        detail = 'timestamped screen attempts: ' + destination.name
    elif args.action == 'watch':
        if not args.seat or not 1 <= args.seconds <= 7200:
            parser.error('watch requires --seat and 1..7200 seconds')
        with destination.open('x') as output:
            watch(proof.data / 'l1.sqlite', proof.data / 'oci-facts.json', output,
                  args.seconds, lambda: proof.guard(force=True), seat=args.seat, computer=args.computer)
        detail = 'L1/recorded host-facts trace: ' + destination.name + '; inspect source observation times for staleness'
    elif args.action == 'snapshot':
        value = snapshot(proof.data / 'l1.sqlite', time.monotonic_ns(), seat=args.seat, computer=args.computer)
        detail = 'read-only L1 snapshot: ' + destination.name
    elif args.action == 'check':
        if not args.after or not args.kind:
            parser.error('check requires --after and --kind')
        try:
            after = ([json.loads(line) for line in args.after.read_text().splitlines()]
                     if args.kind == 'ordering' else json.loads(args.after.read_text()))
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
                elif args.kind == 'ordering':
                    outcome = ordering(after, args.computer)
                elif args.kind == 'control':
                    if not args.before:
                        parser.error('control requires --before ATTEMPTS.jsonl')
                    result = control_timing(after, args.seat, [json.loads(line) for line in args.before.read_text().splitlines()])
                    outcome = result is not None and result['within_one_second']
                else:
                    outcome = warning_before_end(after, args.seat)
                detail = args.kind + (' supporting check passed' if outcome else ' CHECK FAILED; keep row unverified')
                if args.kind == 'control':
                    detail += '; ' + json.dumps(result)
            value = dict(kind=args.kind, outcome=outcome, before=str(args.before), after=str(args.after))
            if args.kind == 'control':
                value['timing'] = result
        except (OSError, ValueError, TypeError, KeyError) as error:
            outcome = False
            detail = 'CHECK FAILED: invalid or missing evidence: ' + str(error)
            value = dict(kind=args.kind, outcome=False, error=str(error), before=str(args.before), after=str(args.after))
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
    if args.action not in ['watch', 'screen-loop']:
        with destination.open('x') as output:
            json.dump(value, output, indent=2)
            output.write('\n')
    with results.open('a') as output:
        verdict = 'FAIL' if args.action == 'check' and outcome is False else 'PARTIAL'
        output.write(json.dumps(dict(row=args.row, detail=detail, receipt=destination.name, verdict=verdict)) + '\n')
    report(repo, [json.loads(line) for line in results.read_text().splitlines()])
    if args.action == 'capture' and result.returncode:
        sys.exit(result.returncode)
    if args.action == 'check' and outcome is False:
        sys.exit(1)


if __name__ == '__main__':
    main()
