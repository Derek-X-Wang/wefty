#!/usr/bin/env python3
"""B1 attended trial. Default/--dry-run prints a plan without side effects.

Worker execution is forbidden. Unit tests use fake service/process seams.
--execute-isolated and --teardown require separate owner authorization.
"""
import sys
sys.dont_write_bytecode = True

import argparse
import base64
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import pwd
import plistlib
import re
import selectors
import shutil
import shlex
import signal
import socket
import subprocess
import tarfile
import time
import uuid

from rolling_upgrade_safety import (Budget, Manifest, PREFIX, atomic_json, clean_env,
    confined, evidence_identity, launchctl, loaded_proof, process_birth, require, require_empty_namespace,
    safe_cli, stop_group, teardown, trial_lock)

NAMES = ("wefty", "wefty-l1", "wefty-l3", "wefty-agent")
PAYLOAD = '''#!/bin/sh
set -eu
probe=$1
pidfile=$2
code=$3
case "$probe" in probe-*) ;; *) exit 91 ;; esac
printf '%s' "$$" > "$pidfile"
i=1
while [ "$i" -le 1800 ]; do
  printf 'out:%04d\n' "$i"
  printf 'err:%04d\n' "$i" >&2
  i=$((i+1))
  sleep 1
done
exit "$code"
'''


def endpoint(address):
    host,port=address.rsplit(":",1)
    require(host=="127.0.0.1" and 49152<=int(port)<=65535 and int(port) not in (47101,47102),"non-proof endpoint")


def instant(value):
    value=re.sub(r"\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)",lambda m:"."+(m[1]+"000000")[:6],value)
    return dt.datetime.fromisoformat(value.replace("Z","+00:00"))


def source_policy():
    return json.loads(Path(__file__).with_name('testdata').joinpath('rolling-upgrade-source-policy.json').read_text())


def resolved_refs(args):
    repo=Path(args.repo).resolve()
    top=subprocess.run(['git','rev-parse','--show-toplevel'],cwd=repo,capture_output=True,text=True,timeout=10)
    require(top.returncode==0,'repository root unavailable')
    repo=Path(top.stdout.strip()).resolve()
    refs=args.refs or ['HEAD']*3
    policy=source_policy();checked={};results=[]
    for ref in refs:
        sha='UNRESOLVED'
        try:
            result=subprocess.run(['git','rev-parse','--verify','--end-of-options',ref+'^{commit}'],cwd=repo,capture_output=True,text=True,timeout=10)
            require(result.returncode==0,'cannot resolve source ref')
            sha=result.stdout.strip();require(re.fullmatch(r'[0-9a-f]{40}',sha),'invalid source commit')
            if sha not in checked:
                # Ref names are not authority. Bound the source tree before
                # capturing any requested commit, then verify its actual bytes.
                tree=subprocess.run(['git','ls-tree','-rlz','--full-tree',sha],cwd=repo,capture_output=True,timeout=10)
                require(tree.returncode==0 and len(tree.stdout)<=1<<20,'source tree inventory unavailable/over budget')
                estimate=10240
                entries=tree.stdout.rstrip(b'\0').split(b'\0')
                require(len(entries)<=8192,'source tree entry budget exceeded')
                for entry in entries:
                    metadata,name=entry.split(b'\t',1);fields=metadata.split()
                    require(len(fields)==4 and fields[0] in (b'100644',b'100755') and fields[1]==b'blob'
                            and len(name)<=4096 and fields[3].isdigit(),'special/invalid source tree entry')
                    estimate+=((int(fields[3])+511)//512+3+(len(name)+511)//512)*512
                require(estimate<=128<<20,'source archive size budget exceeded before capture')
                archive=subprocess.run(['git','archive','--format=tar',sha],cwd=repo,capture_output=True,timeout=30)
                require(archive.returncode==0 and len(archive.stdout)<=128<<20,'source archive unavailable/over budget')
                checked[sha]=compilation_fingerprint(archive.stdout)==policy['compilation_fingerprint']
            results.append((ref,sha,checked[sha]))
        except (RuntimeError,OSError,ValueError,subprocess.TimeoutExpired):results.append((ref,sha,False))
    return repo,results



def harness_identity():
    directory=Path(__file__).resolve().parent
    result=subprocess.run(['git','rev-parse','HEAD'],cwd=directory,capture_output=True,text=True,timeout=10)
    sha=result.stdout.strip()
    require(result.returncode==0 and re.fullmatch(r'[0-9a-f]{40}',sha),'executing harness commit unavailable')
    resources=['scripts/measure-rolling-upgrade.py','scripts/rolling_upgrade_safety.py',
               'scripts/testdata/rolling-upgrade-plan.go.tmpl','scripts/testdata/rolling-upgrade-source-policy.json']
    top=subprocess.run(['git','rev-parse','--show-toplevel'],cwd=directory,capture_output=True,text=True,timeout=10)
    require(top.returncode==0,'executing harness repository unavailable')
    root=Path(top.stdout.strip()).resolve();clean=True
    for name in resources:
        committed=subprocess.run(['git','show',sha+':'+name],cwd=root,capture_output=True,timeout=10)
        require(committed.returncode==0,'committed harness resource unavailable')
        clean=clean and (root/name).read_bytes()==committed.stdout
    return sha,clean

def approval_identity(args):
    owner=pwd.getpwuid(os.getuid());home=Path(owner.pw_dir).resolve()
    evidence=Path(args.evidence_dir).absolute() if getattr(args,'evidence_dir',None) else home/'.local/share'/('wefty-b1-evidence-'+uuid.uuid4().hex)
    root,identity,prefix=evidence_identity(evidence/'manifest.json')
    require(evidence.parent==home/'.local/share','approval evidence must be directly under owner .local/share')
    return home,evidence,root,prefix


def plan(args):
    repo,refs=resolved_refs(args)
    args.harness_sha,args.harness_clean=harness_identity()
    home,evidence,root,prefix=approval_identity(args)
    model='UNAVAILABLE'
    if platform.system()=='Darwin':
        try:
            result=subprocess.run(['/usr/sbin/sysctl','-n','hw.model'],capture_output=True,text=True,timeout=3)
            if result.returncode==0:model=result.stdout.strip() or 'UNAVAILABLE'
        except (OSError,subprocess.TimeoutExpired):pass
    print('B1 PLAN ONLY; no installation or launchd command has run')
    print(f'APPROVAL PACKET: Mac={platform.node()}; model={model}; OS={platform.system()}; UID={os.getuid()}; owner-home={home}; repo={repo}; Python={sys.version.split()[0]} ({sys.executable})')
    print('Harness commit: '+args.harness_sha+'; tracked execution resources: '+('clean' if args.harness_clean else 'MODIFIED; commit/review and regenerate packet before execution'))
    policy=source_policy()
    print('Reviewed compilation fingerprint: '+policy['compilation_fingerprint']+'; provenance commit: '+policy['reviewed_commit'])
    for n,(ref,sha,accepted) in enumerate(refs):print(f'Ref{n}: {ref} => {sha}; compilation policy '+('PASS' if accepted else 'REFUSE'))
    print(f'Runtime: {root} (fresh exclusive 0700; open directory handle)')
    print(f'Evidence directory: {evidence}; manifest: {evidence}/manifest.json (printed before execution; retained)')
    print(f'Single-run lock: {evidence.parent}/.wefty-b1-proof.lock; held throughout trial/recovery/teardown; existing proof root/label => REFUSE new trial')
    print(f'HOME={root}/home; plists={root}/home/Library/LaunchAgents/*.plist; no real ~/Library/LaunchAgents writes')
    print(f'Labels: {prefix}'+'{l1,l3,node-agent,node-agent.g-<generation32hex>}; gui/'+str(os.getuid())+' only')
    print('Ports: 127.0.0.1:59101 (L1), 127.0.0.1:59102 (L3); reserve both up front, release just before setup; collision => STOP')
    print('Builds: three private distinct commits of the exact reviewed refs above; build0 version=b1-proof-bootstrap, build1/2 version=<private target SHA>; go build -trimpath -ldflags=-X main.version=<target> -o <runtime>/build-N/ ./cmd/{wefty,wefty-l1,wefty-l3,wefty-agent}')
    print('Adapters: proof labels/ports/local main/broker/resources/printed-plan/login PATH/temp alias; exec snapshot helper disabled; final owned-root teardown removes snapshots')
    print('Budgets: CPU2 means Go concurrency PER PROCESS (GOMAXPROCS=2, GOFLAGS=-p=1), not a two-CPU aggregate limit; soft Go heap512MiB/service, disk12GiB total, free22GiB initially/10GiB floor, output32MiB (archive128MiB), log8MiB/file, memory20% free, swap growth<=2GiB')
    print('Deadline: 90m trial + <=3m teardown; preparation commands<=20m; each active upgrade conservative whole-command<=90s; exact-Attempt settlement<=120s')
    steps=(
      ("Prepare", "take fixed single-run lock; refuse other proof roots/labels; create evidence-bound manifest/control lease/open root; reserve ports; fingerprint/build/print ALL 3 plans before service writes", "unknown source/plan, collision, budget or signal", "bounded teardown; no installation recovery"),
      ("Install", "private setup from build0; strict broker checks every label/plist/profile and prior absence", "foreign/already-owned label or setup failure", "bounded teardown; never claim a pre-existing label"),
      ("Enroll", "one idle upgrade --rolling to private target0 before payloads", "enrollment refusal or health failure", "save refusal; bounded teardown; no daily action"),
      ("Payload", "two explicit probes: 1800 numbered stdout/stderr records, real exit0/17; bind probe->Run/Job/Attempt/PID", "missing start, successor, restart rise, invalid renewal/log/exit", "bounded teardown; retain PARTIAL evidence"),
      ("Upgrade1/2/3", "private main 0->1->2->0; new primary+operation every time; status/doctor+renewals; cancellation/fresh new-primary Run", "already_at, unchanged identity, missing timing or >90s", "save receipts; bounded teardown; recovery only exact evidence directory + upgrade --rolling if separately scoped"),
      ("Crash", "verify older g2 through live proof label+kernel start; capture ancestry then SIGKILL; poll exact Attempt", "unavailable/reused PID, successor or settlement timeout", "do not signal ambiguous PID; retain pending crashed-generation files"),
      ("Finish", "verify 0/17, all ordered logs, normal retirement cleanup and killed-generation pending evidence", "any failed assertion", "PARTIAL; same bounded teardown"),
      ("Teardown", "block starts; join controller; reload inventory; reconcile live proof namespace; stop only live-verified owned groups; saved sessions block deletion without signal authority; verify absence; descriptor-relative runtime deletion", "unverified service/process, persistence/cleanup error or <=3m deadline", "keep runtime/evidence; incomplete; later --teardown EXACT_EVIDENCE_DIRECTORY retry, never broader kill/delete"))
    for n,(stage,action,stop,recovery) in enumerate(steps,1):print(f"{n}. {stage}: {action}. STOP: {stop}. ACTION: {recovery}.")
    print('Recovery accepts the exact printed evidence directory; derives its sibling root and prefix; mutable manifest fields cannot redirect it. Saved session IDs never authorize signals. Unknown ownership retains runtime and prevents PASS.')
    print('Manual recovery takes the same single-run and lifecycle leases; child stdout/stderr/status are preserved.')
    print('Estimated isolated45-65m warm/65-85m cold; daily separate ONE real distinct upgrade35-50m plus prep; optional owner reboot+10-20m.')
    print('Owner approval: exact Mac/UID/home/repo/evidence/root/labels/ports/refs; builds/downloads/per-process resource budgets; setup/enrollment/three cutovers/probes/cancel/older-agent SIGKILL/evidence capture/owned teardown+retry. Separate approval: daily target/Runs; optional reboot. All live execution NOT-RUN.')
    blockers=[]
    if not args.harness_clean:blockers.append('executing harness resources differ from printed commit')
    if getattr(args,'harness_commit',None) not in (None,args.harness_sha):blockers.append('approved harness commit differs from executing harness')
    if model=='UNAVAILABLE':blockers.append('exact Mac model unavailable; regenerate packet on owner host')
    if sys.version_info<(3,12):blockers.append('Python 3.12+ required for source extraction')
    print('Approval readiness: '+('REFUSE: '+'; '.join(blockers) if blockers else 'machine identity printed; owner review still required'))
    valid=all(item[2] for item in refs)
    print('Execute only after approval: '+shlex.join([sys.executable,str(Path(__file__).resolve()),'--execute-isolated','--repo',str(repo),'--evidence-dir',str(evidence),'--harness-commit',args.harness_sha,'--refs']+[item[1] for item in refs]))
    return valid,repo,refs,evidence,model


def embedded_roots(files):
    # Scan every non-test Go source, conservatively including packages/platform
    # variants beyond the built commands. Literal file/directory patterns cover
    # all reviewed embeds. Glob patterns retain the entire static ancestor:
    # extra bytes may refuse compatibility, but no matching asset is omitted.
    roots=set()
    for name,data in files.items():
        if not name.endswith('.go') or name.endswith('_test.go'):continue
        for match in re.finditer(rb'(?m)^[ \t]*//go:embed[ \t]+([^\r\n]+)',data):
            text=match[1].decode()
            # Go escape/backquote syntax is outside the reviewed grammar.
            # Refuse it rather than interpreting it as a different asset path.
            require('\\' not in text and '`' not in text,'unreviewed embed pattern quoting')
            patterns=shlex.split(text)
            require(patterns,'empty embed pattern')
            for pattern in patterns:
                if pattern.startswith('all:'):pattern=pattern[4:]
                parts=pattern.split('/')
                require(all(part not in ('','.','..') for part in parts),'unsafe embed pattern')
                literal=[]
                for part in parts:
                    if any(char in part for char in '*?['):break
                    literal.append(part)
                roots.add(str(Path(name).parent.joinpath(*literal)))
    return roots


def compilation_fingerprint(archive):
    files={}
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        for entry in tar:
            n=entry.name
            require(not n.startswith("/") and ".." not in Path(n).parts,"unsafe archive path")
            if entry.isdir():continue
            require(entry.isfile(),"linked/special archive entry refused")
            files[n]=tar.extractfile(entry).read()
    roots=embedded_roots(files);entries={}
    for n,data in files.items():
        embedded=any(root=='.' or n==root or n.startswith(root+'/') for root in roots)
        # Test source and harness/docs are compilation exclusions only when
        # they are not also embedded bytes. Embedded names carry no exemption.
        excluded=n.split("/")[0] in ("docs","scripts",".github",".claude",".agents",".codex") or n.endswith("_test.go")
        if excluded and not embedded:continue
        entries[n]=hashlib.sha256(data).hexdigest()
    return hashlib.sha256(json.dumps(entries,sort_keys=True,separators=(",",":")).encode()).hexdigest()


def patch_exec_aliases(wrapper,cleanup,prefix):
    require(wrapper.count("/tmp/wefty-run.XXXXXXXX")==1 and cleanup.count("/tmp/wefty-run.")==2,
            "reviewed exec alias adapter shape changed")
    binding="b1_link_prefix="+shlex.quote(str(prefix))+"\n"
    wrapper=wrapper.replace("/tmp/wefty-run.XXXXXXXX",'"$b1_link_prefix"XXXXXXXX')
    first,rest=wrapper.split("\n",1)
    wrapper=first+"\n"+binding+rest
    cleanup="# B1 proof: preserve exec snapshots for final owned-root cleanup.\nretire_exec_storage() { return 0; }\n"
    return wrapper,cleanup


class Proof:
    def __init__(self,args):
        # No resources are created in the constructor: main's finally owns the
        # object before initialize can encounter a partial-setup failure.
        require(platform.system()=="Darwin" and os.geteuid()!=0,"attended non-root Mac required")
        self.args=args
        owner,self.evidence,self.root,prefix=approval_identity(args)
        parent=self.evidence.parent
        require(parent.is_dir() and parent.stat().st_uid==os.getuid(),"existing owner-owned .local/share required")
        require(not self.root.exists() and not self.evidence.exists(),"proof runtime/evidence must be fresh")
        require(self.root.resolve()==self.root and self.evidence.resolve()==self.evidence,"redirected proof parent")
        self.home=self.root/"home"
        self.source=self.root/"source"
        self.profile=self.home/".config/wefty/profile.json"
        self.bin=self.home/".local/bin/wefty"
        self.manifest=None
        self.ports=[]
        self.serial=0
        self.probes={}
        self.owned=set()
        self.activation_seen={}
        self.upgrade_previous={}
        self.command_starts={}
        self.baseline_swap=None
        self.budget=Budget(time.monotonic()+90*60)
        self.env=clean_env(self.home)
        self.env.update(GOMAXPROCS="2",GOMEMLIMIT="512MiB",GOFLAGS="-p=1",GOENV="off",
            GOCACHE=str(self.root/"cache/go"),GOMODCACHE=str(self.root/"cache/mod"),
            GOPATH=str(self.root/"cache/gopath"),TMPDIR=str(self.root/"tmp"),GOTMPDIR=str(self.root/"tmp"),SHELL="/bin/sh")
        self.env["WEFTY_PROFILE"]=str(self.profile)

    def initialize(self):
        self.manifest=Manifest.create(self.evidence/"manifest.json",self.root,[])
        require_empty_namespace(self.root.parent,lambda args:launchctl(self.manifest,args))
        self.lifecycle=self.manifest.controller()
        self.lifecycle.__enter__()
        self.manifest.mkdir(self.root)
        require(shutil.disk_usage(self.root).free >= 22<<30,"need 22 GiB free before proof preparation")
        for path in (self.home,self.root/"tmp",self.root/"cache",self.root/"cache/go",self.root/"cache/mod",self.root/"cache/gopath"):
            self.manifest.mkdir(path)
        # The approval packet has fixed high proof ports. Reserve BOTH before
        # any build and keep them until immediately before starting services.
        self.port_reservations=[]
        for port in (59101,59102):
            sock=socket.socket();self.port_reservations.append(sock)
            sock.bind(("127.0.0.1",port))
            address="127.0.0.1:"+str(port);endpoint(address);self.ports.append(address)
            with self.manifest.locked() as d:d["ports"]=list(self.ports)
        # The broker is immutable private evidence, not a product binary.
        safety=Path(__file__).with_name("rolling_upgrade_safety.py")
        shutil.copyfile(safety,self.evidence/"rolling_upgrade_safety.py")
        self.broker=self.evidence/"launchctl-proof"
        self.broker.write_text("#!"+sys.executable+"\nimport sys\nsys.path.insert(0,"+repr(str(self.evidence))+")\nfrom rolling_upgrade_safety import Manifest,launchctl\ncode,out=launchctl(Manifest("+repr(str(self.manifest.path))+"),sys.argv[1:])\nsys.stdout.buffer.write(out)\nraise SystemExit(code)\n")
        self.broker.chmod(0o500)
        self.catalog_path=self.home/"Library/Application Support/wefty/agent-generations/catalog.json"
        self.record("resources",{"root":str(self.root),"ports":self.ports,"manifest":str(self.manifest.path)})

    def record(self,kind,value):
        with (self.evidence/"measurements.jsonl").open("a") as out:
            out.write(json.dumps({"at":dt.datetime.now(dt.timezone.utc).isoformat(),"kind":kind,"value":value})+"\n")

    def disk_guard(self,output=0):
        used=0
        for root in (self.root,self.evidence):
            if root.exists():
                for base,_,files in os.walk(root,followlinks=False):
                    for name in files:
                        p=Path(base)/name
                        if not p.is_symlink():
                            try:used+=p.stat().st_size
                            except FileNotFoundError:pass # product atomic publication
        self.budget.check(used=used,free=shutil.disk_usage(self.root).free,output=output)
        for log in self.home.glob("Library/Logs/wefty/*.log"):
            require(log.stat().st_size<=8<<20,"service output cap exceeded")

    def guard(self):
        self.disk_guard()
        pressure=self.probe(["/usr/bin/memory_pressure","-Q"])
        swap=self.probe(["/usr/sbin/sysctl","-n","vm.swapusage"])
        free=re.search(r"System-wide memory free percentage:\s*(\d+)%",pressure)
        used=re.search(r"used\s*=\s*([\d.]+)([KMG])",swap)
        require(free is not None and used is not None,"memory guard unavailable")
        amount=float(used[1])*{"K":1<<10,"M":1<<20,"G":1<<30}[used[2]]
        if self.baseline_swap is None:self.baseline_swap=amount
        self.record("memory",{"free_percent":int(free[1]),"swap_bytes":amount})
        require(int(free[1])>=20 and amount-self.baseline_swap<=2<<30,"memory guard tripped")

    def probe(self,argv):
        # Trusted OS probes are short, bounded subprocess groups too.
        return self.run(argv,"os-probe",timeout=5,guarded=False)

    def raw(self,argv,timeout=30,isolated=True,cwd=None):
        return subprocess.CompletedProcess(argv,0,self.run(argv,"tool",timeout=timeout,cwd=cwd),"")

    def observe_upgrade(self,name):
        path=self.profile.parent/"upgrade-rolling.json"
        if not path.exists():return
        r=json.loads(path.read_text())
        if r["operation_id"]==self.upgrade_previous.get(name):return
        if r["phase"]=="complete":
            start=getattr(self,"command_starts",{}).get(name)
            require(start is not None,"activation timing unverified: missing command boundary")
            elapsed=time.monotonic()-start
            require(elapsed<=90,"conservative whole-upgrade timing exceeds 90 seconds")
            self.record(name+"-timing",{"conservative_upper_seconds":elapsed,"includes_preparation_guard_scheduling":True,"operation_id":r["operation_id"]})
            self.activation_seen[name]=r["operation_id"]
        self.record(name+"-receipt",r)

    def run(self,argv,name,timeout=120,cwd=None,allowed=(0,),guarded=True,binary=False,limit=None,completed=False):
        self.manifest.check_open()
        if guarded:self.guard()
        self.serial+=1
        name=f"{self.serial:04d}-"+uuid.uuid4().hex[:8]+"-"+name
        start=time.monotonic()
        if "upgrade-" in name:
            self.command_starts[name]=start
            path=self.profile.parent/"upgrade-rolling.json"
            if path.exists():self.upgrade_previous[name]=json.loads(path.read_text())["operation_id"]
        maximum=self.budget.max_output if limit is None else limit
        outputs=[self.evidence/(name+suffix) for suffix in (".out",".err")]
        total=0
        child=None
        with outputs[0].open("xb") as out,outputs[1].open("xb") as err:
            try:
                child,birth=self.manifest.spawn([str(x) for x in argv],env=self.env,cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                with selectors.DefaultSelector() as selector:
                    for stream,destination in ((child.stdout,out),(child.stderr,err)):
                        os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,destination)
                    last_guard=start
                    while selector.get_map():
                        self.manifest.check_open()
                        now=time.monotonic()
                        require(now-start<timeout and now<self.budget.deadline,"command/overall deadline exceeded")
                        for key,_ in selector.select(timeout=0.2):
                            chunk=os.read(key.fileobj.fileno(),65536)
                            if not chunk:selector.unregister(key.fileobj);continue
                            total+=len(chunk);require(total<=maximum,"command output cap exceeded")
                            key.data.write(chunk)
                        if now-last_guard>=2:
                            if guarded:self.guard()
                            else:self.disk_guard()
                            if "upgrade-" in name:self.observe_upgrade(name)
                            self.manifest.refresh_owned()
                            last_guard=time.monotonic()
                code=child.wait(timeout=5)
                require(code in allowed,name+" failed, exit "+str(code))
            finally:
                if child is not None:
                    try:
                        stop_group(child.pid,birth=birth,owned_now=child.poll() is None,reap=lambda:child.wait(timeout=2))
                        with self.manifest.locked() as d:d["groups"][str(child.pid)]["active"]=False
                    finally:
                        for stream in (child.stdout,child.stderr):stream.close()
        if "upgrade-" in name:
            self.observe_upgrade(name)
            require(name in self.activation_seen,"activation timing unverified: no new completed operation")
        if not name.endswith("os-probe"):
            self.record(name,{"exit":code,"seconds":time.monotonic()-start,"output_bytes":total})
        if completed:
            return subprocess.CompletedProcess(argv,code,outputs[0].read_text(),outputs[1].read_text())
        return outputs[0].read_bytes() if binary else outputs[0].read_text()

    def cli(self,args,name,as_json=True,allowed=(0,),timeout=120):
        argv,env=safe_cli(self.home,self.profile,self.ports,self.bin,(["--json"] if as_json else [])+args)
        # Resource paths are fixed, while all connection variables come from
        # the sanitized wrapper (also used by manual recovery).
        env.update({k:v for k,v in self.env.items() if not k.startswith("WEFTY_")})
        previous=self.env;self.env=env
        try:
            result=self.run(argv,name,allowed=allowed,timeout=timeout,cwd=self.root/"workload" if (self.root/"workload").exists() else self.root)
        finally:self.env=previous
        return json.loads(result) if as_json else result

    def validate_plan(self,plan):
        require(isinstance(plan,dict) and len(plan.get("services",[]))==3,"invalid service plan")
        d=self.manifest.read()
        require(plan.get("ports")==self.ports and plan.get("source_remote")==str(self.source)
                and plan.get("broker")==str(self.broker),"plan endpoint/source/broker mismatch")
        for path in plan.get("paths",[]):confined(path,self.root)
        require(len(plan.get("paths",[]))==7,"missing plan paths")
        expected={d["prefix"]+name for name in ("l1","l3","node-agent")}
        from rolling_upgrade_safety import validate_definition
        labels=set()
        for encoded in plan["services"]:
            definition=plistlib.loads(base64.b64decode(encoded))
            labels.add(validate_definition(definition,d))
        require(labels==expected,"unexpected service labels")
        return labels

    def publish(self,index):
        ref=self.args.refs[index]
        sha=self.raw(["git","rev-parse","--verify","--end-of-options",ref+"^{commit}"],cwd=self.args.repo).stdout.strip()
        require(re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}",sha),"invalid source commit")
        archive=self.run(["git","archive","--format=tar",sha],"archive",cwd=self.args.repo,binary=True,limit=128<<20)
        policy=json.loads(Path(__file__).with_name("testdata").joinpath("rolling-upgrade-source-policy.json").read_text())
        require(compilation_fingerprint(archive)==policy["compilation_fingerprint"],"source is outside reviewed compilation fingerprint; refuse before mutation")
        self.manifest.mkdir(self.source)
        for path in self.source.iterdir():
            if path.name!=".git":shutil.rmtree(path) if path.is_dir() else path.unlink()
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:source.extractall(self.source,filter="data")
        prefix=self.manifest.read()["prefix"]
        replacements={"dev.wefty.":prefix,"127.0.0.1:47101":self.ports[0],"127.0.0.1:47102":self.ports[1],
                      "https://github.com/Derek-X-Wang/wefty.git":str(self.source),"/bin/launchctl":str(self.broker)}
        changed=[]
        for path in self.source.rglob("*.go"):
            if ".git" in path.parts or path.name.endswith("_test.go"):continue
            old=path.read_text();new=old
            for before,after in replacements.items():new=new.replace(before,json.dumps(after,ensure_ascii=False)[1:-1])
            if old!=new:path.write_text(new);changed.append(str(path.relative_to(self.source)))
        wrapper_path=self.source/"cmd/wefty/exec_wrapper.sh"
        cleanup_path=self.source/"cmd/wefty/exec_cleanup.sh"
        wrapper,cleanup=patch_exec_aliases(wrapper_path.read_text(),cleanup_path.read_text(),self.root/"tmp/wefty-run.")
        wrapper_path.write_text(wrapper);cleanup_path.write_text(cleanup)

        changed.extend(["cmd/wefty/exec_wrapper.sh","cmd/wefty/exec_cleanup.sh","cmd/wefty/b1_proof_plan.go"])
        setup=self.source/"cmd/wefty/setup_command.go"
        content=setup.read_text()
        require(content.count("loginShell: runLoginShell")==1 and content.count("agents = append(agents, service)")==1,"reviewed setup adapter shape changed")
        content=content.replace("loginShell: runLoginShell","loginShell: b1LoginShell").replace("agents = append(agents, service)","b1Resources(&service)\n        agents = append(agents, service)")
        setup.write_text(content)
        resource_keys=("B1_PROOF_ID","GOMAXPROCS","GOMEMLIMIT","GOFLAGS","GOCACHE","GOMODCACHE","GOPATH","TMPDIR","GOTMPDIR","GOENV")
        env={k:self.env[k] for k in resource_keys}
        envliteral="{"+",".join(json.dumps(k)+":"+json.dumps(v) for k,v in env.items())+"}"
        template=Path(__file__).with_name("testdata").joinpath("rolling-upgrade-plan.go.tmpl").read_text()
        template=template.replace("@@ENV@@",envliteral).replace("@@PATH@@",json.dumps(self.env["PATH"]))
        (self.source/"cmd/wefty/b1_proof_plan.go").write_text(template)
        (self.source/"b1-proof-generation.txt").write_text(str(index)+"\n")
        if not (self.source/".git").exists():self.raw(["git","init","-b","main"],cwd=self.source)
        self.raw(["git","add","."],cwd=self.source)
        self.raw(["git","-c","user.name=B1 Proof","-c","user.email=proof@example.invalid","-c","core.hooksPath=/dev/null","-c","commit.gpgsign=false","commit","-m",f"B1 isolated version {index}"],cwd=self.source)
        target=self.raw(["git","rev-parse","HEAD"],cwd=self.source).stdout.strip()
        self.record("source",{"ref":ref,"original_commit":sha,"target":target,"substitutions":replacements,"adapter_files":changed})
        return target

    def catalog(self):
        c = json.loads(self.catalog_path.read_text())
        require(c.get("primary_generation_id"), "catalog lacks primary")
        for g in c["generations"].values():
            require(g["label"].startswith(PREFIX), "daily launchd label refused")
            for key in ("executable", "plist"):
                path = Path(g[key])
                require(path.is_relative_to(self.home) and path.resolve() == path,
                        "generation-owned path escaped proof HOME")
                self.owned.add(str(path))
        self.record("catalog", c)
        return c

    def snapshot(self, name):
        crash = getattr(self, "crash_diagnostics", None)
        status_codes = (12,) if crash else (0, 1, 5, 12)
        doctor_codes = (12,) if crash else (0, 1, 5)
        status = self.cli(["status"], name + "-status", allowed=status_codes)
        self.cli(["status"], name + "-status-text", as_json=False, allowed=status_codes)
        doctor = self.cli(["doctor"], name + "-doctor", allowed=doctor_codes)
        self.cli(["doctor"], name + "-doctor-text", as_json=False, allowed=doctor_codes)
        self.catalog()
        if crash:
            self.verify_crash_diagnostics(name, status, doctor, crash)
        if re.fullmatch(r"after-[123]", name):
            expected = int(name[-1])
            retiring = [g for g in status.get("generations", []) if g["role"] == "retiring"]
            require(status.get("upgrade_state") == "complete" and doctor.get("healthy") is True,
                    "status/doctor did not report healthy completed overlap")
            require(len(retiring) >= expected, "status omitted a live retiring generation")

    def verify_crash_diagnostics(self, name, status, doctor, expected):
        require(status.get("ready") is False and doctor.get("healthy") is False,
                "crash diagnostic incorrectly reports ready/healthy")
        original = self.probes[expected["run"]]
        terminal = ("lost", "interrupted", "failed")
        cancelled_present = {}
        for kind, report in (("status", status), ("doctor", doctor)):
            generations = report.get("generations", [])
            matches = [g for g in generations if g.get("agent_generation_id") == expected["generation"]]
            require(len(matches) == 1, "crash diagnostic omitted/duplicated killed generation: " + kind)
            attempts = [a for a in matches[0].get("attempts", []) if a.get("attempt_id") == original["attempt"]]
            require(len(attempts) == 1 and attempts[0].get("job_id") == original["job"]
                    and attempts[0].get("run_id") == expected["run"] and attempts[0].get("state") in terminal,
                    "crash diagnostic omitted exact killed Attempt/Run and terminal state: " + kind)
            problems = report.get("reasons", []) if kind == "status" else [
                p for p in report.get("findings", []) if p.get("severity") != "ok"]
            detail = ("generation " + expected["generation"] + ": Run " + expected["run"]
                      + " (Attempt " + original["attempt"] + "): " + attempts[0]["state"])
            require(any(p.get("code", p.get("check")) == "retiring_attempt_problem"
                        and p.get("detail") == detail for p in problems),
                    "crash diagnostic missing killed Attempt problem with exact generation/Run: " + kind)
            for run in expected["survivors"]:
                attempt = self.probes[run]["attempt"]
                # Match whole identifiers, including any structured problem
                # fields, rather than accepting an unrelated Attempt prefix.
                pattern = r"(?<![\w-])(?:" + re.escape(attempt) + "|" + re.escape(run) + r")(?![\w-])"
                require(not any(re.search(pattern, json.dumps(p)) for p in problems),
                        "surviving probe reported as a problem: " + kind + " " + run + " " + attempt)
            cancelled = expected["cancelled"]
            pattern = r"(?<![\w-])(?:" + re.escape(self.probes[cancelled]["attempt"]) + "|" + re.escape(cancelled) + r")(?![\w-])"
            # Current product also lists the owner-cancelled Run. Record this
            # known nit without requiring it, so its eventual removal passes.
            cancelled_present[kind] = any(re.search(pattern, json.dumps(p)) for p in problems)
        self.record(name + "-diagnostics", {"killed_generation": expected["generation"],
                    "killed_run": expected["run"], "killed_attempt": original["attempt"],
                    "surviving_attempts": [self.probes[run]["attempt"] for run in expected["survivors"]],
                    "cancelled_run": expected["cancelled"], "cancelled_problem_present": cancelled_present})

    def inspect(self, run):
        return self.cli(["inspect", run, "--execution"], "inspect-" + run)

    def started(self, run):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            observation = self.inspect(run)
            job = (observation.get("execution") or {}).get("job")
            if job and job["state"] == "running":
                require(len(job.get("attempts", [])) == 1, "initial Attempt is not unique")
                pidfile = Path(self.probes[run]["pidfile"])
                if pidfile.exists():
                    facts = {"run": run, "job": job["job_id"], "attempt": job["current_attempt_id"],
                             "restart_count": (job.get("restart") or {}).get("count", 0),
                             "pid": pidfile.read_text(), "lease": job["attempts"][0]["lease_expires_at"]}
                    self.probes[run].update(facts)
                    # Payload PID is measurement evidence only. Ownership must
                    # be independently rooted in a live proof-label process.
                    self.manifest.refresh_owned()
                    self.record("started", facts)
                    return facts
            time.sleep(2)
        raise RuntimeError("payload never started: " + run)

    def submit(self, code=0):
        probe = "probe-" + uuid.uuid4().hex
        pidfile = self.root / (probe + ".pid")
        if hasattr(self, "manifest"):
            self.manifest.intent(pidfile, "payload-pid")
            with self.manifest.locked() as d:d["probes"][probe]={"pidfile":str(pidfile),"run_id":None}
        result = self.cli(["exec", "--detach", "--", "/bin/sh", str(self.root / "workload/payload.sh"),
                           probe, str(pidfile), str(code)], "submit")
        run = result["run_id"]
        self.probes[run] = {"probe": probe, "pidfile": str(pidfile)}
        if hasattr(self,"manifest"):
            with self.manifest.locked() as d:d["probes"][probe]["run_id"]=run
        self.record("probe-run-binding", {"run": run, **self.probes[run]})
        self.started(run)
        return run

    def survival(self, run, boundary):
        before = self.probes[run]
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            observation = self.inspect(run)
            job = observation["execution"]["job"]
            require(job["job_id"] == before["job"] and job["current_attempt_id"] == before["attempt"]
                    and len(job["attempts"]) == 1, "replacement Attempt is not survival")
            require((job.get("restart") or {}).get("count", 0) == before["restart_count"], "restart count rose")
            require(Path(before["pidfile"]).read_text() == before["pid"], "payload PID changed")
            os.kill(int(before["pid"]), 0)
            a = job["attempts"][0]
            if instant(a["updated_at"]) > instant(boundary) and instant(a["lease_expires_at"]) > instant(before["lease"]):
                self.record("survival", {"identity": before, "renewal": a, "after": boundary})
                return
            time.sleep(2)
        raise RuntimeError("no post-cutover renewal for original Attempt")

    def verify_terminal(self, run, code):
        self.cli(["wait", run], "wait-" + run, timeout=2100, allowed=(0, 10))
        job = self.inspect(run)["execution"]["job"]
        original = self.probes[run]
        require(len(job["attempts"]) == 1 and job["attempts"][0]["attempt_id"] == original["attempt"], "terminal successor refused")
        require(job["attempts"][0]["result"]["exit_code"] == code, "real exit code differs")
        raw = self.cli(["logs", run, "--all"], "logs-" + run, as_json=False)  # human-readable copy
        events = self.run([self.bin, "--json", "logs", run, "--all"], "log-events-" + run)
        streams = {"stdout": b"", "stderr": b""}
        sequence = {}
        for line in events.splitlines():
            e = json.loads(line)
            require(e["attempt_id"] == original["attempt"] and not e.get("gap"), "log gap or replacement Attempt")
            stream = e["stream"]
            if stream in sequence:
                require(e["sequence"] == sequence[stream] + 1, "log sequence is not contiguous")
            sequence[stream] = e["sequence"]
            streams[stream] += base64.b64decode(e.get("bytes", ""))
        for stream, prefix in (("stdout", "out"), ("stderr", "err")):
            numbered = re.findall(rb"(?m)^" + prefix.encode() + rb":(\d{4})$", streams[stream])
            require(numbered == [f"{i:04d}".encode() for i in range(1, 1801)], "numbered output lost, duplicated or reordered")
        self.record("terminal", {"run": run, "exit": code, "records_per_stream": 1800,
                                 "human_log_bytes": len(raw)})

    def wait_attempt(self,run,state,timeout=120):
        deadline=time.monotonic()+timeout
        last=None
        while time.monotonic()<deadline:
            self.guard()
            last=self.inspect(run)["execution"]["job"]
            original=self.probes[run]
            require(last["job_id"]==original["job"] and len(last.get("attempts",[]))==1
                    and last["attempts"][0]["attempt_id"]==original["attempt"],"replacement Attempt during settlement")
            attempt=last["attempts"][0]
            if attempt["state"]==state:
                if state=="lost":require(not attempt.get("result"),"invented crash exit")
                self.record("settled",{"run":run,"job":last})
                return last
            time.sleep(1)
        self.record("settlement-timeout",{"run":run,"last_observation":last})
        raise RuntimeError("exact Attempt settlement unverified at deadline")

    def require_cutover(self,before,after,result):
        require(not result.get("already_at") and before["primary_generation_id"]!=after["primary_generation_id"]
                and bool(after.get("operation_id")) and before.get("operation_id")!=after["operation_id"],
                "upgrade was a no-op or primary/promotion operation did not change")

    def cutover_identity(self):
        c=self.catalog()
        r=json.loads((self.profile.parent/"upgrade-rolling.json").read_text())
        return {"primary_generation_id":c["primary_generation_id"],"operation_id":r["operation_id"]}

    def verify_retirement_cleanup(self,catalog,crashed,normal):
        for generation in normal:
            retained=catalog["generations"].get(generation["agent_generation_id"])
            require(retained is None or retained.get("role")!="retiring","normal retiree has not closed")
            require(not Path(generation["executable"]).exists() and not Path(generation["plist"]).exists(),"normal retiree owned files remain")
        for identity in crashed:
            g=catalog["generations"].get(identity)
            require(g is not None and g["role"]=="retiring" and Path(g["executable"]).exists()
                    and Path(g["plist"]).exists(),"killed retiree pending evidence was incorrectly removed")
        self.record("product-retirement-cleanup",{"normal":normal,"expected_pending_crashed":sorted(crashed)})

    def execute(self):
        self.guard()
        targets=[];builds=[];plans=[]
        for index in range(3):
            target=self.publish(index);targets.append(target)
            build=self.root/("build-"+str(index));self.manifest.mkdir(build);builds.append(build)
            for name in NAMES:self.manifest.intent(build/name,"binary")
            version="b1-proof-bootstrap" if index==0 else target
            self.run(["go","build","-trimpath","-ldflags=-X main.version="+version,"-o",str(build)+"/"]+["./cmd/"+n for n in NAMES],"build",timeout=1200,cwd=self.source)
            plan=json.loads(self.run([build/"wefty","--b1-print-plan"],"preflight-plan"))
            plans.append(plan);labels=self.validate_plan(plan)
            for path in plan["paths"]:
                if not Path(path).exists():self.manifest.intent(path,"file" if Path(path).suffix==".json" else "directory")
            for name in NAMES:self.manifest.intent(self.home/".local/bin"/name,"binary")
            for label in labels:self.manifest.intent(self.home/"Library/LaunchAgents"/(label+".plist"),"plist")
        with self.manifest.locked() as d:
            d["validated"]=targets
            d["plan_receipts"]=plans
        for name in ("l1","l3","node-agent"):
            launchctl(self.manifest,["print","gui/"+str(os.getuid())+"/"+self.manifest.read()["prefix"]+name])
        self.raw(["git","reset","--hard",targets[0]],cwd=self.source)
        for sock in self.port_reservations:sock.close()
        self.port_reservations=[]
        self.run([builds[0]/"wefty","setup","--from",builds[0],"--node-id","b1-proof-node"],"setup",timeout=180)
        self.cli(["upgrade","--rolling"],"idle-enrollment",timeout=1200)
        catalogs=list(self.home.rglob("agent-generations/catalog.json"));require(len(catalogs)==1,"ambiguous generation catalog")
        self.catalog_path=catalogs[0]
        work=self.root/"workload";self.manifest.mkdir(work)
        (work/"payload.sh").write_text(PAYLOAD)
        self.raw(["git","init"],cwd=work);self.raw(["git","add","."],cwd=work)
        self.raw(["git","-c","user.name=B1 Proof","-c","user.email=proof@example.invalid","-c","commit.gpgsign=false","-c","core.hooksPath=/dev/null","commit","-m","Explicit B1 probes"],cwd=work)
        success,failure=self.submit(0),self.submit(17)
        cancel_probe=crash_probe=None
        originals=[];crashed=set();crash_generation=None
        for index,target in enumerate((targets[1],targets[2],targets[0]),1):
            c=self.catalog();originals.append(c["generations"][c["primary_generation_id"]])
            before=self.cutover_identity()
            self.raw(["git","reset","--hard",target],cwd=self.source)
            result=self.cli(["upgrade","--rolling"],f"upgrade-{index}",timeout=1200)
            self.require_cutover(before,self.cutover_identity(),result)
            boundary=dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00","Z")
            self.survival(success,boundary);self.survival(failure,boundary);self.snapshot("after-"+str(index))
            if index==1:cancel_probe=self.submit()
            elif index==2:
                crash_probe=self.submit()
                c=self.catalog();crash_generation=c["generations"][c["primary_generation_id"]]
        self.cli(["cancel",cancel_probe],"cancel-retiree")
        cancelled=self.wait_attempt(cancel_probe,"failed",timeout=120)
        require(cancelled.get("outcome")=="canceled","cancellation outcome missing")
        # Four shared Slots are full until cancellation settles. This fresh
        # primary Run then starts while g0 and g2 still hold their work.
        fresh=self.cli(["exec","--detach","--","/bin/sh","-c","exit 0"],"new-primary")["run_id"]
        self.cli(["wait",fresh],"new-primary-complete")
        self.record("new-primary-run",self.inspect(fresh))
        c=self.catalog();g=c["generations"][crash_generation["agent_generation_id"]]
        require(g["role"]=="retiring" and g["process"]==crash_generation["process"],"crash target changed")
        code,observed=launchctl(self.manifest,["print","gui/"+str(os.getuid())+"/"+g["label"]])
        require(code==0 and re.search(rb"\bpid = "+str(g["process"]["pid"]).encode()+rb"\b",observed),"launchd PID mismatch")
        loaded_proof(self.manifest,self.manifest.snapshot(),g["label"],lambda args:launchctl(self.manifest,args))
        self.manifest.refresh_owned()
        pid=g["process"]["pid"];identity=self.manifest.children.get(pid)
        require(identity is not None and identity["source"]=="launchd" and process_birth(pid)==identity["birth"],"older-agent identity changed before SIGKILL")
        os.kill(pid,signal.SIGKILL);crashed.add(g["agent_generation_id"])
        self.record("older-agent-killed",g)
        self.wait_attempt(crash_probe,"lost",timeout=120)
        cancelled=self.wait_attempt(cancel_probe,"failed",timeout=120)
        require(cancelled.get("outcome")=="canceled","cancellation outcome missing")
        self.crash_diagnostics={"generation":g["agent_generation_id"],"run":crash_probe,
                                "survivors":[success,failure],"cancelled":cancel_probe}
        self.snapshot("after-crash")
        self.verify_terminal(success,0);self.verify_terminal(failure,17)
        normal=[g for g in originals if g["agent_generation_id"] not in crashed]
        deadline=time.monotonic()+120
        while True:
            self.guard();c=self.catalog()
            try:self.verify_retirement_cleanup(c,crashed,normal);break
            except RuntimeError:
                if time.monotonic()>=deadline:raise
                time.sleep(2)
        self.record("verdict","PASS measurement; killed generation retains pending evidence; stock/reboot NOT-RUN")

    def teardown(self):
        for sock in getattr(self,"port_reservations",[]):sock.close()
        self.port_reservations=[]
        if self.manifest is not None:teardown(self.manifest)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run",action="store_true")
    mode.add_argument("--execute-isolated",action="store_true")
    mode.add_argument("--teardown",metavar="EVIDENCE_DIRECTORY")
    mode.add_argument("--proof-command",metavar="EVIDENCE_DIRECTORY")
    parser.add_argument("--repo",default=".")
    parser.add_argument("--refs",nargs=3)
    parser.add_argument("--harness-commit",help="executing harness SHA frozen in approved packet")
    parser.add_argument("--evidence-dir",help="exact fresh directory printed by approved dry-run packet")
    parser.add_argument("command",nargs=argparse.REMAINDER)
    args=parser.parse_args()
    if args.teardown:
        saved={sig:signal.signal(sig,signal.SIG_IGN) for sig in (signal.SIGINT,signal.SIGTERM)}
        try:
            m=Manifest(Path(args.teardown)/'manifest.json')
            m.snapshot() # bind the approved evidence name before requesting stop
            # Request cooperative stop before waiting for the whole-trial lock.
            fd=os.open(m.path.with_suffix('.lifecycle'),os.O_RDWR|os.O_NOFOLLOW)
            try:os.pwrite(fd,b'X',0)
            except OSError as error:m.persistence_errors.append('control: '+str(error))
            finally:os.close(fd)
            started=time.monotonic()
            with trial_lock(m.path.parent.parent,timeout=175):
                teardown(m,deadline_seconds=max(3,180-(time.monotonic()-started)))
        finally:
            for sig,handler in saved.items():signal.signal(sig,handler)
        return
    if args.proof_command:
        m=Manifest(Path(args.proof_command)/"manifest.json");d=m.read();home=Path(d["root"])/"home"
        require(not d["closing"],"proof is closing; use teardown, not recovery")
        command=args.command[1:] if args.command[:1]==["--"] else args.command
        require(command and command[0] in ("cancel","upgrade","inspect","logs","status","doctor"),"manual command outside allowlist")
        argv,env=safe_cli(home,home/".config/wefty/profile.json",d["ports"],home/".local/bin/wefty",command)
        if command[0]=="upgrade":require(command==["upgrade","--rolling"],"only exact rolling recovery is allowed")
        proof=Proof.__new__(Proof)
        proof.manifest=m;proof.root=Path(d["root"]);proof.home=home;proof.evidence=Path(d["evidence"])
        proof.profile=home/".config/wefty/profile.json";proof.bin=home/".local/bin/wefty";proof.ports=d["ports"]
        proof.env=env;proof.serial=0;proof.baseline_swap=None;proof.command_starts={};proof.upgrade_previous={};proof.activation_seen={}
        proof.budget=Budget(time.monotonic()+120)
        # Manual recovery is bounded and journaled but is not arrival timing
        # evidence. It gets the same sanitized endpoints/resource/output limits.
        def interrupted_manual(signum,frame):
            for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
            raise SystemExit(128+signum)
        saved={sig:signal.signal(sig,interrupted_manual) for sig in (signal.SIGINT,signal.SIGTERM)}
        try:
            with trial_lock(m.path.parent.parent),m.controller():
                # Re-read endpoints and closing state AFTER taking the lease.
                d=m.read()
                m.require_idle(d)
                argv,env=safe_cli(home,home/".config/wefty/profile.json",d["ports"],home/".local/bin/wefty",command)
                proof.env=env
                result=proof.run(argv,"manual-command",timeout=120,allowed=range(-127,256),completed=True)
                sys.stdout.write(result.stdout);sys.stderr.write(result.stderr)
                return result.returncode if result.returncode>=0 else 128-result.returncode
        finally:
            for sig,handler in saved.items():signal.signal(sig,handler)
    valid,repo,refs,evidence,model=plan(args)
    if not args.execute_isolated:return 0 if valid else 1
    require(valid,'source policy REFUSE; no trial resources created')
    require(args.harness_clean and args.harness_commit==args.harness_sha,'executing harness changed/unapproved; regenerate packet after commit/review')
    require(model!='UNAVAILABLE','Mac model unavailable; approval packet incomplete')
    require(sys.version_info>=(3,12),'Python 3.12+ required; no trial resources created')
    require(args.evidence_dir is not None,'execution requires the exact --evidence-dir from the approved packet')
    args.repo=str(repo);args.refs=[item[1] for item in refs];args.evidence_dir=str(evidence)
    sys.stdout.flush() # exact manifest/evidence identity visible before execution
    proof=Proof(args)
    with trial_lock(evidence.parent):return controlled_trial(proof)


def controlled_trial(proof):
    previous={};measured=False;cleanup_entered=False
    def interrupted(signum,frame):
        for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
        raise SystemExit(128+signum)
    try:
        for sig in (signal.SIGINT,signal.SIGTERM):previous[sig]=signal.signal(sig,interrupted)
        proof.initialize() # acquires the controller lease before runtime mkdir
        try:
            proof.execute();measured=True
        finally:
            for sig in previous:signal.signal(sig,signal.SIG_IGN)
            cleanup_entered=True
            try:
                proof.teardown()
                proof.record("final-verdict",{"status":"PASS" if measured else "PARTIAL","known_owned_services_and_groups":"absent","runtime":"removed"})
            except BaseException as error:
                try:proof.record("final-verdict",{"status":"PARTIAL","teardown_error":str(error)})
                except OSError:pass
                raise
    finally:
        try:
            if not cleanup_entered and proof.manifest is not None:
                for sig in previous:signal.signal(sig,signal.SIG_IGN)
                proof.teardown()
        finally:
            if getattr(proof,"lifecycle",None) is not None:proof.lifecycle.__exit__(None,None,None)
            for sig,handler in previous.items():signal.signal(sig,handler)


if __name__=="__main__":raise SystemExit(main())
