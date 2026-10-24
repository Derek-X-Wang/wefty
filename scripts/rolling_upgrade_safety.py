"""Safety boundary for the attended B1 harness; no effects at import time."""
import contextlib
import ctypes
import fcntl
import json
import os
from pathlib import Path
import plistlib
import re
import signal
import stat
import subprocess
import sys
import time
import uuid

PREFIX = "dev.wefty-b1-proof."


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def confined(path, root):
    path, root = Path(path), Path(root)
    require(path.is_absolute() and path != root and root in path.parents,
            "path outside proof root: " + str(path))
    require(path.resolve() == path, "redirected proof path: " + str(path))
    return path


def validate_ports(ports,complete=True):
    require(isinstance(ports,list) and (len(ports)==2 if complete else len(ports)<=2)
            and len(set(ports))==len(ports),"invalid proof ports")
    for address in ports:
        match=re.fullmatch(r"127\.0\.0\.1:(\d+)",address)
        require(match is not None and 49152<=int(match[1])<=65535,"non-proof/default port refused")


def clean_env(home):
    env = {k: v for k, v in os.environ.items()
           if k in ("PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "USER", "LOGNAME", "TERM")}
    env.update(HOME=str(home), XDG_STATE_HOME=str(Path(home) / ".local/state"))
    if re.fullmatch(r"wefty-b1-proof-[0-9a-f]{32}",Path(home).parent.name):
        env["B1_PROOF_ID"]=Path(home).parent.name[-32:]
    return env


def safe_cli(home, profile, ports, binary, args):
    validate_ports(ports)
    confined(profile, home)
    confined(binary, home)
    p = json.loads(Path(profile).read_text())
    require(p.get("fabric","plain")=="plain","non-plain proof profile refused")
    require([p.get("l1_address"), p.get("l3_address")] == ports,
            "saved proof endpoints disagree with profile")
    require(not any(str(a).startswith(("--profile", "--l1", "--l3", "--fabric", "--node-config")) for a in args),
            "connection override refused")
    env = clean_env(home)
    env["WEFTY_PROFILE"] = str(profile)
    root=Path(home).parent
    env.update(GOMAXPROCS="2",GOMEMLIMIT="512MiB",GOFLAGS="-p=1",GOENV="off",
               GOCACHE=str(root/"cache/go"),GOMODCACHE=str(root/"cache/mod"),
               GOPATH=str(root/"cache/gopath"),TMPDIR=str(root/"tmp"),GOTMPDIR=str(root/"tmp"))
    return [str(binary), "--profile", str(profile), "--fabric", "plain",
            "--l1", ports[0], "--l3", ports[1]] + list(args), env


class Budget:
    def __init__(self, deadline, max_output=32 << 20, max_disk=12 << 30,
                 min_free=10 << 30, now=time.monotonic):
        self.deadline, self.max_output = deadline, max_output
        self.max_disk, self.min_free, self.now = max_disk, min_free, now

    def check(self, used, free, output=0):
        require(self.now() < self.deadline, "overall deadline exceeded")
        require(used <= self.max_disk, "proof disk budget exceeded")
        require(free >= self.min_free, "proof free-space floor reached")
        require(output <= self.max_output, "command output budget exceeded")


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(value, out)
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temp.exists():
            temp.unlink()


@contextlib.contextmanager
def atomic_spawn():
    # Defer Python termination exceptions until the returned PID is recorded.
    # This does not block the child's signal mask or make it inherit SIG_IGN.
    saved={sig:signal.getsignal(sig) for sig in (signal.SIGINT,signal.SIGTERM)}
    pending=[]
    def defer(signum,frame):pending.append(signum)
    for sig,handler in saved.items():
        if handler!=signal.SIG_IGN:signal.signal(sig,defer)
    try:yield
    finally:
        for sig,handler in saved.items():signal.signal(sig,handler)
        if pending:
            sig=pending[0];handler=saved[sig]
            if callable(handler):handler(sig,None)
            elif handler!=signal.SIG_IGN:raise SystemExit(128+sig)


def evidence_identity(path):
    """The caller-approved evidence directory, never JSON, selects one run."""
    path=Path(path).absolute()
    require(path.name=='manifest.json' and path.resolve()==path,'redirected evidence path')
    match=re.fullmatch(r'wefty-b1-evidence-([0-9a-f]{32})',path.parent.name)
    require(match is not None,'expected exact proof evidence directory')
    identity=match[1]
    return path.parent.parent/('wefty-b1-proof-'+identity),identity,PREFIX+identity+'.'


@contextlib.contextmanager
def trial_lock(parent,timeout=0):
    """One fixed owner-account lock, outside every disposable proof root."""
    parent=Path(parent)
    require(parent.resolve()==parent and parent.stat().st_uid==os.getuid(),'unsafe proof parent')
    fd=os.open(parent/'.wefty-b1-proof.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and info.st_nlink==1
                and stat.S_IMODE(info.st_mode)==0o600,'unsafe global proof lock')
        deadline=time.monotonic()+timeout
        while True:
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);break
            except BlockingIOError:
                require(time.monotonic()<deadline,'another proof controller/teardown holds the single-run lock')
                time.sleep(.02)
        yield
    finally:os.close(fd)


def gui_domain_labels(out,domain):
    """Recognize one complete GUI-domain diagnostic, or refuse absence.

    launchctl print is not a stable API. A changed grammar is incomplete
    evidence, never an empty namespace. Other domain sections (notably Mach
    endpoints shared with user/<uid>) are not service ownership evidence.
    """
    require(len(out)<=1<<20,'proof namespace output bound exceeded')
    lines=[line.strip() for line in out.decode().splitlines() if line.strip()]
    require(len(lines)>=2 and lines[0]==domain+' = {' and lines[-1]=='}','proof GUI namespace header/domain unavailable')
    depth=1;inside=False;blocks=0;expected=None;kind=None;labels=set()
    for line in lines[1:-1]:
        if inside:
            if line=='}':inside=False;depth-=1;continue
            fields=line.split()
            require(len(fields)==3 and re.fullmatch(r'-|[0-9]+',fields[0])
                    and re.fullmatch(r'-|[+-]?[0-9]+|\(pe\)|\(jt\)',fields[1])
                    and re.fullmatch(r'[^\s{}"=]+',fields[2]),'proof GUI service inventory malformed')
            require(fields[2] not in labels,'duplicate GUI service label')
            labels.add(fields[2]);continue
        if depth==1:
            if re.match(r'services\b',line):
                require(line=='services = {' and blocks==0,'unknown/additional GUI services block')
                blocks+=1;inside=True;depth+=1;continue
            if line.startswith('service count ='):
                match=re.fullmatch(r'service count = ([0-9]+)',line)
                require(expected is None and match is not None,'GUI service count unavailable/duplicate')
                expected=int(match[1]);continue
            if line.startswith('type ='):
                require(kind is None and line in ('type = gui','type = login'),'unexpected GUI domain type')
                kind=line.split(' = ',1)[1];continue
        # Ignore metadata values and other balanced sections, not service rows.
        # Quoted metadata cannot introduce structural braces.
        structural=re.sub(r'"(?:\\.|[^"\\])*"','""',line)
        depth+=structural.count('{')-structural.count('}')
        require(depth>=1,'proof GUI namespace truncated/malformed')
    require(depth==1 and not inside and blocks==1 and kind in ('gui','login')
            and expected is not None and expected==len(labels),'proof GUI service enumeration incomplete')
    return {label for label in labels if label.startswith(PREFIX)}


def proof_labels(invoke,uid=None):
    domain='gui/'+str(os.getuid() if uid is None else uid)
    code,out=invoke(['print',domain])
    require(code==0,'proof GUI namespace inspection unavailable')
    return gui_domain_labels(out,domain)


def require_empty_namespace(parent,invoke):
    require(not list(Path(parent).glob('wefty-b1-proof-*')),'stale/other proof root exists; inspect/teardown that evidence directory first')
    require(not proof_labels(invoke),'stale/other proof label exists; new run refused')


class Manifest:
    """Journal is evidence. Only an open root and live ownership authorize effects."""
    @classmethod
    def create(cls,path,root,ports,uid=None):
        path,root=Path(path).absolute(),Path(root)
        require(not path.exists() and not root.exists(),'proof resources must be new')
        require(root.resolve()==root and re.fullmatch(r'wefty-b1-proof-[0-9a-f]{32}',root.name),'invalid proof root')
        expected,identity,prefix=evidence_identity(path)
        require(root==expected,'evidence directory/root identity mismatch')
        path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        fd=os.open(path.with_suffix('.lifecycle'),os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600)
        try:os.write(fd,b'R');os.fsync(fd)
        finally:os.close(fd)
        data={'version':2,'id':root.name[-32:],'uid':os.getuid() if uid is None else uid,
              'root':str(root),'evidence':str(path.parent),'prefix':PREFIX+root.name[-32:]+'.',
              'ports':ports,'paths':{},'labels':{},'groups':{},'probes':{},'validated':[],
              'closing':False,'teardown':'pending','root_identity':None}
        atomic_json(path,data)
        m=cls(path);m.persisted={}
        m.persist(data)
        return m

    def __init__(self,path):
        self.path=Path(path).absolute()
        self.cache=None;self.persisted={};self.root_fd=None;self.controller_fd=None;self.creating_root=False
        self.owned={};self.children={};self.persistence_errors=[];self.uncertain=[];self.ambiguous_groups=set()
        self.reuse_observations=set()

    def validate(self,d):
        require(d['version']==2 and d['uid']==os.getuid(),'foreign/stale proof manifest')
        root=Path(d['root'])
        expected,identity,prefix=evidence_identity(self.path)
        require(root==expected and d['id']==identity and d['prefix']==prefix,'evidence directory/run identity mismatch')
        require(root.is_absolute() and root.resolve()==root and re.fullmatch(r'wefty-b1-proof-[0-9a-f]{32}',root.name)
                and d['id']==root.name[-32:] and d['prefix']==PREFIX+d['id']+'.'
                and str(self.path.parent)==d['evidence'] and root not in self.path.parents,'manifest identity mismatch')
        validate_ports(d['ports'],complete=bool(d['validated']))
        for name,entry in d['paths'].items():
            path=Path(name)
            require(path.is_absolute() and '..' not in path.parts and (path==root or root in path.parents),
                    'manifest deletion target outside proof root')
            require(path.parent.resolve()==path.parent,'manifest path has redirected parent')
            require(entry['kind'] in ('directory','file','binary','plist','payload-pid','symlink'),'invalid path kind')
            identity=entry.get('identity')
            require(identity is None or (isinstance(identity,list) and len(identity)==2 and all(type(v)==int and v>=0 for v in identity)), 'invalid path identity')
        for label,entry in d['labels'].items():
            require(re.fullmatch(re.escape(d['prefix'])+r'(?:l1|l3|node-agent(?:\.g-[0-9a-f]{32})?)',label),'foreign label in manifest')
            if entry.get('definition'):require(validate_definition(entry['definition'],d)==label,'saved definition mismatch')
        # Saved membership is context-independent data. A broker inherits its
        # caller's recorded group; reading it grants no live signal authority.
        # Self-group rejection belongs at registration/shutdown boundaries.
        for pgid,entry in d['groups'].items():
            require(str(pgid).isdigit() and int(pgid)>1
                    and isinstance(entry,dict) and isinstance(entry.get('birth'),str)
                    and type(entry.get('sid',int(pgid)))==int and entry.get('sid',int(pgid))==int(pgid),'invalid saved process/session')
        identity=d.get('root_identity')
        if root.exists():
            info=root.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid==d['uid'] and stat.S_IMODE(info.st_mode)==0o700,
                    'unsafe proof root')
            require(identity==[info.st_dev,info.st_ino] or (identity is None and self.creating_root and self.root_fd is not None
                    and (os.fstat(self.root_fd).st_dev,os.fstat(self.root_fd).st_ino)==(info.st_dev,info.st_ino)), 'stale/replaced proof root')
        return d

    def read(self):
        info=self.path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)==0o600,'unsafe manifest')
        d=self.validate(json.loads(self.path.read_text()))
        self.cache=json.loads(json.dumps(d))
        self.persisted=json.loads(json.dumps(d))
        return d

    @contextlib.contextmanager
    def locked(self):
        fd=os.open(self.path.with_suffix('.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            deadline=time.monotonic()+5
            while True:
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:
                    require(time.monotonic()<deadline,'journal lock deadline exceeded');time.sleep(.02)
            d=self.read()
            yield d
            self.validate(d)
            self.cache=json.loads(json.dumps(d)) # ownership snapshot precedes persistence
            self.persist(d)
        finally:os.close(fd)

    def persist(self,d):
        self.validate(d)
        self.cache=json.loads(json.dumps(d))
        event={}
        for key,value in d.items():
            if key in ('paths','labels','groups','probes'):
                delta={k:v for k,v in value.items() if self.persisted.get(key,{}).get(k)!=v}
                if delta or key not in self.persisted:event[key]=delta
            elif self.persisted.get(key)!=value:event[key]=value
        try:
            if event:
                log=os.open(self.path.with_suffix('.ownership'),os.O_CREAT|os.O_APPEND|os.O_WRONLY|os.O_NOFOLLOW,0o600)
                try:
                    info=os.fstat(log)
                    require(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600,'unsafe ownership journal')
                    payload=(json.dumps(event)+'\n').encode()
                    if os.write(log,payload)!=len(payload):raise OSError('short ownership append')
                    os.fsync(log)
                finally:os.close(log)
            atomic_json(self.path,d)
            self.persisted=json.loads(json.dumps(d))
        except OSError as error:self.persistence_errors.append(str(error));raise

    def snapshot(self):
        try:return self.read()
        except OSError as error:
            self.persistence_errors.append(str(error))
            # Replay bounded append-only deltas, independently validating the
            # complete result. A malformed summary is never silently ignored.
            d={}
            try:
                log=os.open(self.path.with_suffix('.ownership'),os.O_RDONLY|os.O_NOFOLLOW)
                with os.fdopen(log,'r') as stream:
                    require(os.fstat(stream.fileno()).st_size<=32<<20,'ownership journal bound exceeded')
                    for line in stream:
                        require(line.endswith('\n'),'incomplete ownership event')
                        for key,value in json.loads(line).items():
                            if key in ('paths','labels','groups','probes'):d.setdefault(key,{}).update(value)
                            else:d[key]=value
                return self.validate(d)
            except OSError:
                require(self.cache is not None,'no validated ownership snapshot')
                return self.validate(json.loads(json.dumps(self.cache)))

    def open_root(self,d=None):
        d=self.snapshot() if d is None else d
        if self.root_fd is None:
            require(d['root_identity'] is not None,'root lacks creation identity')
            self.root_fd=os.open(d['root'],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        info=os.fstat(self.root_fd)
        require([info.st_dev,info.st_ino]==d['root_identity'] and info.st_uid==os.getuid(),'open root identity mismatch')
        return self.root_fd

    def intent(self,path,kind):
        path=Path(path)
        with self.locked() as d:
            root=Path(d['root'])
            if path!=root:confined(path,root)
            require(not d['closing'],'proof closing')
            if str(path) not in d['paths']:
                require(not path.exists() and not path.is_symlink(),'resource exists')
                d['paths'][str(path)]={'kind':kind,'identity':None}

    def created(self,path):
        path=Path(path);info=path.lstat()
        with self.locked() as d:
            require(str(path) in d['paths'] and info.st_uid==os.getuid() and not path.is_symlink(),'unowned resource')
            d['paths'][str(path)]['identity']=[info.st_dev,info.st_ino]
            if str(path)==d['root']:d['root_identity']=[info.st_dev,info.st_ino]
        if str(path)==self.cache['root']:self.open_root(self.cache)

    def mkdir(self,path):
        path=Path(path)
        if path.exists():return
        self.intent(path,'directory');path.mkdir(mode=0o700)
        if str(path)==self.cache['root']:
            self.root_fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);self.creating_root=True
        try:self.created(path)
        finally:self.creating_root=False

    def check_open(self):
        require(self.controller_fd is not None,'command requires lifecycle lock')
        require(os.pread(self.controller_fd,1,0)==b'R','proof closing; controller must stop')

    @contextlib.contextmanager
    def controller(self,closing=False,timeout=5):
        # The one lifecycle lock covers the WHOLE controller/recovery command,
        # including its finally. Teardown requests stop, then joins this lease.
        self.snapshot() # reject complete malformed manifest before ANY effect
        fd=os.open(self.path.with_suffix('.lifecycle'),os.O_RDWR|os.O_NOFOLLOW)
        info=os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)==0o600,'unsafe lifecycle lock')
        try:
            if closing:
                try:os.pwrite(fd,b'X',0) # preallocated control byte, not evidence
                except OSError as error:self.persistence_errors.append('control: '+str(error))
            deadline=time.monotonic()+timeout
            while True:
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:
                    require(time.monotonic()<deadline,'controller has not joined; teardown incomplete')
                    time.sleep(.02)
            require(closing or os.pread(fd,1,0)==b'R','proof closing')
            self.controller_fd=fd
            yield
        finally:self.controller_fd=None;os.close(fd)

    def group(self,pgid,birth,kind='command'):
        self.check_open()
        require(pgid>1 and pgid!=os.getpgrp(),'unsafe spawned group')
        # This method is used ONLY immediately after our own Popen. It records
        # ownership in memory BEFORE any fallible journal operation.
        self.owned[pgid]=birth
        self.children[pgid]={'birth':birth,'pgid':pgid,'source':'spawn'}
        with self.locked() as d:d['groups'][str(pgid)]={'birth':birth,'kind':kind,'active':True,'sid':pgid}

    def require_idle(self,d):
        self.check_open()
        require(not self.session_candidates(d),'saved command session present/unverified; recovery refused')
        for pgid in d['groups']:
            require(not self.saved_group_present(int(pgid),d),'previous command group present/unverified; recovery refused; use teardown/inspection')

    def spawn(self,argv,**kwargs):
        self.check_open();child=None;birth=''
        try:
            with atomic_spawn():
                child=subprocess.Popen(argv,start_new_session=True,**kwargs)
                birth=process_birth(child.pid)
                self.owned[child.pid]=birth
                self.children[child.pid]={'birth':birth,'pgid':child.pid,'source':'spawn'}
            self.group(child.pid,birth)
            return child,birth
        except BaseException:
            if child is not None:
                stop_group(child.pid,birth,owned_now=child.poll() is None,reap=lambda:child.wait(timeout=.5))
            raise

    def known_births(self,d):
        births={int(pid):entry['birth'] for pid,entry in d['groups'].items()}
        births.update(self.owned)
        births.update({pid:entry['birth'] for pid,entry in self.children.items()})
        return births

    def pid_reused(self,pid,saved,birth=None):
        if not saved:return False
        birth=process_birth(pid) if birth is None else birth
        if not birth or birth==saved:return False
        observation=(pid,saved,birth)
        if observation not in self.reuse_observations:
            self.reuse_observations.add(observation)
            try:
                with self.locked() as d:d['pid_reused']={'pid':pid,'saved_birth':saved,'observed_birth':birth}
            except OSError as error:
                if str(error) not in self.persistence_errors:self.persistence_errors.append(str(error))
        return True

    def saved_group_present(self,pgid,d):
        # A group number is not process identity. Exclude only members whose
        # saved birth is positively different; unknown members still block.
        births=self.known_births(d)
        members=[row[1] for row in process_rows() if row[0]==os.getuid() and row[-1]==pgid]
        if members:
            return any(not self.pid_reused(pid,births.get(pid)) for pid in members)
        try:os.killpg(pgid,0)
        except ProcessLookupError:return False
        return True # kernel group exists but inspection supplied no members

    def session_candidates(self,d):
        # Saved SIDs are conservative survivor witnesses after controller loss.
        # They NEVER admit a PID to children or authorize any signal.
        sessions={entry.get('sid',int(pgid)) for pgid,entry in d['groups'].items()}|set(self.owned)
        if not sessions:return []
        births=self.known_births(d)
        candidates=[]
        for row in process_rows():
            if row[0]!=os.getuid():continue
            pid=row[1]
            if pid in births:
                birth=process_birth(pid)
                if self.pid_reused(pid,births[pid],birth):continue
                if not birth:
                    try:os.kill(pid,0)
                    except ProcessLookupError:continue
                    except PermissionError:pass
                candidates.append(pid);continue # same or unavailable identity, even if SID changed
            try:sid=os.getsid(pid)
            except ProcessLookupError:continue # confirmed disappearance
            except OSError:candidates.append(pid);continue # unknown != absent
            if sid in sessions:candidates.append(pid)
        return sorted(set(candidates))

    def refresh_owned(self):
        rows=process_rows() # UID, PID, PPID, PGID; no argv or environment
        indexed={row[1]:row for row in rows if row[0]==os.getuid()}
        verified={};uncertain=[];ambiguous=set();reused=set()
        for pid,entry in list(self.children.items()):
            birth=process_birth(pid)
            if self.pid_reused(pid,entry['birth'],birth):reused.add(pid);continue
            if pid not in indexed:
                try:os.kill(pid,0)
                except ProcessLookupError:continue
                except PermissionError:pass
                uncertain.append(pid);continue
            if not birth:uncertain.append(pid);ambiguous.add(entry['pgid']);continue
            verified[pid]=entry
        # Only kernel ancestry from a still-verified owned process admits a
        # descendant. No path, argv, environment or saved assertion admits one.
        changed=True
        while changed:
            changed=False
            for pid,row in indexed.items():
                if pid in verified or pid in reused or len(row)!=4 or row[2] not in verified:continue
                birth=process_birth(pid)
                if not birth:uncertain.append(pid);ambiguous.add(row[3]);continue
                verified[pid]={'birth':birth,'pgid':row[3],'source':'ancestry'};changed=True
        # A direct command's new session can outlive its leader. Unrecorded
        # members of that session are candidates, never adopted or signalled.
        # This catches escaped groups without argv/path-based discovery.
        for pid,row in indexed.items():
            if pid in verified or pid in reused:continue
            try:sid=os.getsid(pid)
            except OSError:continue # known unavailable PIDs were retained above
            if sid in self.owned:uncertain.append(pid);ambiguous.add(row[-1])
        self.children.update(verified)
        self.uncertain=sorted(set(uncertain))
        groups={}
        for pid,entry in verified.items():groups.setdefault(entry['pgid'],{})[pid]=entry['birth']
        for pgid,members in groups.items():
            # Inspect EVERY member; an unrelated inspector never authorizes its
            # group, even if another member is genuinely owned.
            actual={pid for pid,row in indexed.items() if row[-1]==pgid}
            if actual!=set(members):
                self.uncertain.extend(sorted(actual-set(members)));ambiguous.add(pgid)
        self.ambiguous_groups=ambiguous
        return groups


def preserve_closed_records(manifest,d,deadline):
    # Capture only private state records AFTER shutdown; no builds/cache scan.
    destination=Path(d['evidence'])/('closed-records-'+uuid.uuid4().hex)
    destination.mkdir(mode=0o700)
    copied=0
    def visit(directory,relative):
        nonlocal copied
        for name in os.listdir(directory):
            require(time.monotonic()<deadline,'record capture deadline exceeded')
            info=os.stat(name,dir_fd=directory,follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
                try:visit(child,relative/name)
                finally:os.close(child)
            elif stat.S_ISREG(info.st_mode) and Path(name).suffix in ('.json','.jsonl','.plist','.log'):
                if info.st_size>8<<20 or copied+info.st_size>128<<20:continue
                target=destination/relative/name;target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                current=os.stat(name,dir_fd=directory,follow_symlinks=False)
                require((current.st_dev,current.st_ino)==(info.st_dev,info.st_ino),'record changed during preservation')
                # Both roots are siblings on the same filesystem. Moving closed
                # records preserves them without doubling disk usage at the cap.
                os.rename(name,target,src_dir_fd=directory)
                copied+=info.st_size
    for relative in ('home/.config/wefty','home/Library/LaunchAgents','home/Library/Application Support/wefty','home/Library/Logs/wefty'):
        directory=os.dup(manifest.open_root(d))
        try:
            for component in Path(relative).parts:
                child=os.open(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
                os.close(directory);directory=child
            visit(directory,Path(relative))
        except FileNotFoundError:pass
        finally:os.close(directory)


def delete_open_tree(manifest,d,deadline):
    """No saved path is used for deletion. All operations are relative to FDs."""
    root=Path(d['root']);fd=manifest.open_root(d)
    def clear(directory):
        os.fchmod(directory,os.fstat(directory).st_mode|0o700)
        for name in os.listdir(directory):
            require(time.monotonic()<deadline,'deletion deadline exceeded')
            info=os.stat(name,dir_fd=directory,follow_symlinks=False)
            require(info.st_uid==d['uid'],'foreign child retained')
            if stat.S_ISDIR(info.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
                try:
                    require((info.st_dev,info.st_ino)==(os.fstat(child).st_dev,os.fstat(child).st_ino),'replaced child directory')
                    clear(child)
                finally:os.close(child)
                os.rmdir(name,dir_fd=directory)
            else:os.unlink(name,dir_fd=directory) # symlinks/hardlinks never followed/chmodded
    clear(fd)
    parent=os.open(root.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        info=os.stat(root.name,dir_fd=parent,follow_symlinks=False)
        require([info.st_dev,info.st_ino]==d['root_identity'],'root moved/replaced')
        os.rmdir(root.name,dir_fd=parent)
    finally:os.close(parent)


class BSDInfo(ctypes.Structure):
    # Apple's public proc_bsdinfo ABI; inspect only, never mutate a process.
    _fields_ = [(name,ctypes.c_uint32) for name in
        ("flags","status","xstatus","pid","ppid","uid","gid","ruid","rgid","svuid","svgid","reserved")]+[
        ("comm",ctypes.c_char*16),("name",ctypes.c_char*32)]+[
        (name,ctypes.c_uint32) for name in ("nfiles","pgid","jobc","tdev","tpgid","nice")]+[
        ("seconds",ctypes.c_uint64),("microseconds",ctypes.c_uint64)]


def process_birth(pid):
    if sys.platform!="darwin":return ""
    lib=ctypes.CDLL("/usr/lib/libproc.dylib",use_errno=True)
    lib.proc_pidinfo.argtypes=[ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int]
    lib.proc_pidinfo.restype=ctypes.c_int
    info=BSDInfo()
    count=lib.proc_pidinfo(pid,3,0,ctypes.byref(info),ctypes.sizeof(info)) # PROC_PIDTBSDINFO
    if count!=ctypes.sizeof(info) or info.pid!=pid or info.uid!=os.getuid() or info.seconds<=0 or info.microseconds>=1000000:return ""
    return f"darwin:{pid}:{info.seconds}:{info.microseconds}"



def validate_definition(p, d):
    label = p.get("Label", "")
    require(re.fullmatch(re.escape(d["prefix"]) + r"(?:l1|l3|node-agent(?:\.g-[0-9a-f]{32})?)", label),
            "service label outside allowlist")
    root = Path(d["root"])
    for key in ("WorkingDirectory", "StandardOutPath", "StandardErrorPath"):
        confined(p.get(key, ""), root)
    argv = p.get("ProgramArguments", [])
    require(argv and Path(argv[0]).name in ("wefty-l1", "wefty-l3", "wefty-agent"), "unapproved service program")
    confined(argv[0], root)
    expected="wefty-"+("agent" if ".node-agent" in label else label.rsplit(".",1)[-1])
    require(Path(argv[0]).name==expected,"service label/program mismatch")
    for arg in argv[1:]:
        require(not arg.startswith("/"),"unexpected absolute service argument")
        if "=" in arg:
            value = arg.split("=", 1)[1]
            if value.startswith("/"):
                confined(value, root)
    env = p.get("EnvironmentVariables", {})
    require(env.get("B1_PROOF_ID")==d["id"],"missing service process identity tag")
    require(env.get("HOME")==str(root/"home"),"unexpected service HOME")
    if "WEFTY_LAUNCH_UNIT" in env:require(env["WEFTY_LAUNCH_UNIT"]==label,"launch unit label mismatch")
    for key in ("HOME", "TMPDIR", "GOCACHE", "GOMODCACHE", "GOPATH"):
        confined(env.get(key, ""), root)
    require(env.get("GOMAXPROCS") == "2" and env.get("GOMEMLIMIT") == "512MiB"
            and env.get("GOFLAGS") == "-p=1", "missing service resource settings")
    require(not any(k.startswith("WEFTY_") and k != "WEFTY_LAUNCH_UNIT" for k in env), "unexpected service connection override")
    return label



def launchctl(manifest, args, invoke=None, teardown=False, deadline=None):
    args = list(args)
    require(args, "missing launchctl action")
    with manifest.locked() as d:
        domain = "gui/" + str(d["uid"])
        if invoke is None:
            def journal(pid,birth,active):
                require(pid>1 and pid!=os.getpgrp(),'unsafe spawned group')
                manifest.owned[pid]=birth
                manifest.children[pid]={"birth":birth,"pgid":pid,"source":"spawn"}
                old=d["groups"].get(str(pid),{})
                d["groups"][str(pid)]={"birth":birth or old.get("birth"),"kind":"launchctl","active":active,"sid":pid}
                manifest.persist(d)
            def invoke(command):
                timeout=10 if deadline is None else min(10,max(0.05,deadline-time.monotonic()-3))
                return system_launchctl(command,timeout=timeout,journal=journal,env=clean_env(Path(d["root"])/"home"))
        action = args[0]
        require(action in ("print", "print-disabled", "enable", "disable", "bootstrap", "bootout", "kickstart"),
                "launchctl action outside allowlist")
        if action == "print" and args==[action,domain]:
            code,out=invoke(args)
            require(code==0,'proof GUI namespace inspection unavailable')
            gui_domain_labels(out,domain)
            return code,out
        if action == "print-disabled":
            require(args == [action, domain], "foreign launchd domain")
            code, output = invoke(args)
            require(code == 0, "cannot inspect proof start overrides")
            # Only return this manifest's labels to the product parser.
            lines = [line for line in output.decode().splitlines()
                     if any(('"' + label + '"') in line for label in d["labels"])]
            return 0, ("disabled services = {\n" + "\n".join(lines) + "\n}\n").encode()
        definition = None
        if action == "bootstrap":
            require(len(args) == 3 and args[1] == domain, "foreign bootstrap domain")
            path = confined(args[2], Path(d["root"]))
            definition = plistlib.loads(path.read_bytes())
            label = validate_definition(definition, d)
            profile_args=[arg.split("=",1)[1] for arg in definition["ProgramArguments"] if arg.startswith("--profile=")]
            require(len(profile_args)==1,"service lacks exact profile")
            profile=confined(profile_args[0],Path(d["root"]))
            settings=json.loads(profile.read_text())
            require(settings.get("fabric")=="plain" and [settings.get("l1_address"),settings.get("l3_address")]==d["ports"],"service effective profile endpoint mismatch")
            expected = Path(d["root"]) / "home/Library/LaunchAgents" / (label + ".plist")
            require(path == expected, "plist path outside allowlist")
        else:
            require(len(args) == 2 and args[1].startswith(domain + "/"), "foreign launchctl target")
            label = args[1][len(domain) + 1:]
        require(re.fullmatch(re.escape(d["prefix"]) + r"(?:l1|l3|node-agent(?:\.g-[0-9a-f]{32})?)", label),
                "launchctl label outside allowlist")
        require(not d["closing"] or teardown or action == "print", "proof teardown in progress")
        if label not in d["labels"]:
            require(not teardown, "teardown cannot acquire labels")
            # Persist reservation BEFORE inspection or any service mutation.
            d["labels"][label] = {"initial_absent": False, "mutated": False}
            manifest.persist(d)
            code, output = invoke(["print", domain + "/" + label])
            require(code == 113, "label already exists or absence is unverified: " + label)
            d["labels"][label]["initial_absent"] = True
            manifest.persist(d)
        entry = d["labels"][label]
        require(entry["initial_absent"], "label was already owned before proof")
        if action != "print":
            control=os.open(manifest.path.with_suffix(".lifecycle"),os.O_RDONLY|os.O_NOFOLLOW)
            try:require(os.read(control,1)==b"R","proof closing")
            finally:os.close(control)
            require(teardown or len(d["validated"]) == 3, "all three version plans must validate before service mutation")
            entry["mutated"] = True
            if definition:
                entry["definition"] = definition
            manifest.persist(d)
        code, output = invoke(args)
        if action == "print" and code == 0:
            match = re.search(rb"\bpid = (\d+)", output)
            if match:
                entry["pid"] = int(match[1])
        return code, output



def stop_group(pgid,birth=None,timeout=5,owned_now=False,reap=None,witness_pid=None,verify=None):
    require(pgid>1 and pgid!=os.getpgrp(),'unsafe group')
    def exists():
        try:os.killpg(pgid,0);return True
        except ProcessLookupError:return False
    def check():
        if verify is not None:verify();return
        pid=pgid if witness_pid is None else witness_pid
        require(birth and process_birth(pid)==birth and os.getpgid(pid)==pgid,'group identity unavailable/reused')
    if not exists():return
    check();os.killpg(pgid,signal.SIGTERM)
    if reap:
        try:reap()
        except subprocess.TimeoutExpired:pass
    until=time.monotonic()+timeout
    while exists() and time.monotonic()<until:time.sleep(.02)
    if exists():
        # Revalidate immediately before escalation, even if TERM was verified.
        check();os.killpg(pgid,signal.SIGKILL)
        if reap:
            try:reap()
            except subprocess.TimeoutExpired:pass
    until=time.monotonic()+2
    while exists() and time.monotonic()<until:time.sleep(.02)
    require(not exists(),'owned group remains')


def system_launchctl(args,timeout=10,journal=None,env=None):
    child=None;birth=''
    try:
        with atomic_spawn():
            child=subprocess.Popen(['/bin/launchctl']+list(args),stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,env=env)
            birth=process_birth(child.pid)
            if journal:journal(child.pid,birth,True)
        out,err=child.communicate(timeout=timeout)
        require(len(out)+len(err)<=1<<20,'service output cap exceeded')
        return child.returncode,out+err
    finally:
        if child is not None:
            stop_group(child.pid,birth,owned_now=child.poll() is None,timeout=0,reap=lambda:child.wait(timeout=.2))
            if journal:journal(child.pid,birth,False)



def process_rows():
    result=subprocess.run(['/bin/ps','-axo','uid=,pid=,ppid=,pgid='],capture_output=True,text=True,timeout=3)
    require(result.returncode==0 and len(result.stdout)<=1<<20,'process inventory unavailable')
    rows=[tuple(map(int,line.split())) for line in result.stdout.splitlines()]
    require(all(len(row)==4 for row in rows),'invalid process inventory')
    return rows


def process_executable(pid):
    lib=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
    lib.proc_pidpath.argtypes=[ctypes.c_int,ctypes.c_void_p,ctypes.c_uint32]
    lib.proc_pidpath.restype=ctypes.c_int
    buffer=ctypes.create_string_buffer(4096)
    require(lib.proc_pidpath(pid,buffer,len(buffer))>0,'owned process path unavailable')
    return os.fsdecode(buffer.value)


def loaded_proof(manifest,d,label,invoke):
    """Saved absence/mutation/PID flags NEVER authorize shutdown by themselves."""
    target='gui/'+str(d['uid'])+'/'+label
    code,out=invoke(['print',target])
    if code==113:return None
    require(code==0,'proof label state unavailable')
    path=Path(d['root'])/'home/Library/LaunchAgents'/(label+'.plist')
    confined(path,Path(d['root']))
    definition=plistlib.loads(path.read_bytes())
    require(validate_definition(definition,d)==label,'live proof definition mismatch')
    # launchd must confirm it loaded THIS owned file and program. A tampered
    # journal cannot claim an existing service by supplying a plausible PID.
    fields={key:value.strip().strip('"') for key,value in re.findall(r'^\s*(path|program) = (.+)$',out.decode(),re.M)}
    require(fields.get('path')==str(path) and fields.get('program')==definition['ProgramArguments'][0], 'launchd definition ownership unverified')
    match=re.search(rb'\bpid = (\d+)',out)
    if match:
        pid=int(match[1]);birth=process_birth(pid)
        require(birth and process_executable(pid)==definition['ProgramArguments'][0] and process_birth(pid)==birth,'launchd process identity unverified')
        pgid=os.getpgid(pid)
        manifest.children[pid]={'birth':birth,'pgid':pgid,'source':'launchd'}
    return target


def teardown(manifest,invoke=None,stop=stop_group,deadline_seconds=180):
    deadline=time.monotonic()+max(0,deadline_seconds-3)
    d=manifest.snapshot() # complete validation BEFORE control-byte/service effects
    require(str(os.getpgrp()) not in d['groups'],'teardown cannot run inside a recorded command group')
    errors=list(manifest.persistence_errors)
    own_lease=manifest.controller_fd is None
    context=manifest.controller(closing=True,timeout=max(0,deadline-time.monotonic())) if own_lease else contextlib.nullcontext()
    with context:
        d=manifest.snapshot() # reload AFTER joining all in-flight controller/broker work
        errors.extend(error for error in manifest.persistence_errors if error not in errors)
        try:os.pwrite(manifest.controller_fd,b'X',0) # permanently blocks new starts
        except OSError as error:errors.append('control: '+str(error))
        def save(**fields):
            try:
                with manifest.locked() as current:current.update(fields)
            except OSError as error:errors.append('journal: '+str(error))
        save(closing=True,teardown='running')
        call_deadline=deadline
        if invoke is None:
            def journal(pid,birth,active):
                require(pid>1 and pid!=os.getpgrp(),'unsafe spawned group')
                manifest.owned[pid]=birth
                manifest.children[pid]={'birth':birth,'pgid':pid,'source':'spawn'}
                try:
                    with manifest.locked() as current:
                        current['groups'][str(pid)]={'birth':birth,'kind':'launchctl','active':active,'sid':pid}
                except OSError as error:errors.append('shutdown journal: '+str(error))
            def invoke(args):
                require(time.monotonic()<call_deadline,'shutdown deadline exceeded')
                return system_launchctl(args,timeout=min(5,max(.05,call_deadline-time.monotonic()-3)),journal=journal,env=clean_env(Path(d['root'])/'home'))
        backend=invoke
        attempt=uuid.uuid4().hex
        sequence=0
        target_prefix='gui/'+str(d['uid'])+'/'+d['prefix']
        def invoke(args):
            nonlocal sequence
            recorded=len(args)==2 and args[0] in ('print','bootout') and args[1].startswith(target_prefix)
            def observation(status,output):
                nonlocal sequence
                sequence+=1
                # One bounded delta per response preserves repeats without a
                # growing snapshot. Journal failure never prevents shutdown.
                save(teardown_observation={'attempt':attempt,'sequence':sequence,'target':args[1],
                     'action':args[0],'status':status,'output':output[:1024]})
            try:status,output=backend(args)
            except BaseException as error:
                if recorded:observation(None,str(error))
                raise
            if recorded:observation(status,output[:1024].decode(errors='replace'))
            return status,output
        # Independently validate live labels before bootout; collect the owned
        # process ancestry while those label PIDs still exist.
        targets=[]
        live=set()
        try:
            live=proof_labels(invoke,d["uid"])
            require(all(label.startswith(d['prefix']) for label in live),'other proof namespace present; retained')
            require(live<=set(d['labels']),'live proof labels omitted from inventory; retained')
        except BaseException as error:errors.append('namespace: '+str(error))
        for label in sorted(set(d['labels'])|{label for label in live if label.startswith(d['prefix'])},key=lambda label:('node-agent' not in label,'.l1' in label)):
            try:
                require(time.monotonic()<deadline,'shutdown deadline exceeded')
                entry=d['labels'].get(label)
                if entry is None:
                    # Missing journal entries cannot authorize shutdown. Keep
                    # files and report incomplete; never claim namespace absent.
                    raise RuntimeError('unrecorded live proof label retained: '+label)
                if not entry.get('initial_absent') or not entry.get('mutated'):
                    require(invoke(['print','gui/'+str(d['uid'])+'/'+label])[0]==113,'pre-existing label left untouched')
                    continue
                target=loaded_proof(manifest,d,label,invoke)
                if target:targets.append(target)
            except BaseException as error:errors.append(str(error))
        try:manifest.refresh_owned()
        except BaseException as error:errors.append('owned inventory: '+str(error))
        for target in targets:
            try:
                invoke(['bootout',target])
                # bootout may return while the job is still exiting. Only the
                # explicit no-service status is absence evidence; all other
                # responses remain unverified until this bounded wait ends.
                until=min(deadline,time.monotonic()+30)
                call_deadline=until
                while True:
                    require(time.monotonic()<until,'proof label still loaded/unverified: '+target)
                    if invoke(['print',target])[0]==113:break
                    remaining=until-time.monotonic()
                    require(remaining>0,'proof label still loaded/unverified: '+target)
                    time.sleep(min(.1,remaining))
            except BaseException as error:errors.append(str(error))
            finally:call_deadline=deadline
        try:
            groups=manifest.refresh_owned()
            for pgid,members in groups.items():
                def verify(pgid=pgid):
                    current=manifest.refresh_owned()
                    require(pgid not in manifest.ambiguous_groups and pgid in current,'owned group membership unavailable')
                    for pid,birth in current[pgid].items():
                        require(process_birth(pid)==birth and os.getpgid(pid)==pgid,'process identity changed before signal')
                require(time.monotonic()<deadline,'shutdown deadline exceeded')
                try:stop(pgid,timeout=min(3,max(0,deadline-time.monotonic()-2)),verify=verify)
                except BaseException as error:errors.append(str(error))
            require(not proof_labels(invoke,d['uid']),'proof namespace still loaded/unverified')
            remaining=manifest.refresh_owned()
            require(not remaining and not manifest.uncertain,'owned survivors unavailable/present: '+str(manifest.uncertain or sorted(remaining)))
            candidates=manifest.session_candidates(d)
            manifest.uncertain=sorted(set(manifest.uncertain)|set(candidates))
            require(not candidates,'saved session survivors unavailable/present: '+str(candidates))
            # Saved groups without live creation/ancestry authority are NEVER
            # signalled. A possible survivor blocks file deletion and PASS.
            for pgid in set(map(int,d['groups']))|set(manifest.owned):
                # A leader's disappearance is not its group's disappearance.
                # Saved inactive flags cannot replace this kernel observation.
                require(not manifest.saved_group_present(pgid,d),'saved process group ownership unverified; retained')
        except BaseException as error:errors.append(str(error))
        # Records are already separate. No archive traversal precedes shutdown.
        # Raw runtime files remain whenever evidence/ownership is incomplete.
        errors.extend(error for error in manifest.persistence_errors if error not in errors)
        if not errors and Path(d['root']).exists():
            try:
                preserve_closed_records(manifest,d,deadline)
                delete_open_tree(manifest,d,deadline)
            except BaseException as error:errors.append(str(error))
        save(teardown='incomplete' if errors else 'absent',teardown_errors=errors)
        require(not errors,'bounded shutdown incomplete: '+'; '.join(errors))
