"""Fake CLI in a runner-created session; all service calls use a fake broker backend."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

manifest=Path(os.environ['B1_FIXTURE_MANIFEST'])
broker=Path(os.environ['B1_FIXTURE_BROKER'])
# Wait for the runner's real spawn registration, reproducing the installed
# failure deterministically rather than racing the parent's journal write.
deadline=time.monotonic()+5
while True:
    data=json.loads(manifest.read_text())
    if str(os.getpgrp()) in data['groups']:break
    if time.monotonic()>deadline:raise RuntimeError('fixture command group was not registered')
    time.sleep(.005)
assert os.getpid()==os.getpgrp()==os.getsid(0),'fixture was not spawned in a fresh session'
result=subprocess.run([sys.executable,str(broker),'print','gui/'+str(os.getuid())+'/'+data['prefix']+'l1'],capture_output=True,text=True,timeout=5)
sys.stdout.write(result.stdout);sys.stderr.write(result.stderr)
if result.returncode!=113:raise SystemExit(result.returncode or 97)

# Other helper reads share the same manifest path; reading saved membership
# does not confer ownership of any group to this fresh Manifest instance.
sys.path.insert(0,str(manifest.parent))
import rolling_upgrade_safety as safety
m=safety.Manifest(manifest)
m.read();m.snapshot()
with m.locked() as saved:assert str(os.getpgrp()) in saved['groups']
assert not m.owned and not m.children,'saved groups became signal authority'
effects=[]
try:safety.teardown(m,invoke=lambda args:effects.append(args),stop=lambda *args,**kwargs:effects.append(args))
except RuntimeError as error:assert 'inside a recorded command group' in str(error)
else:raise AssertionError('helper accepted shutdown inside its recorded group')
assert not effects and not m.read()['closing'],'helper shutdown had effects'

def forbidden_signal(*args,**kwargs):raise AssertionError('helper tried to signal its own group')
safety.os.killpg=forbidden_signal
try:safety.stop_group(os.getpgrp(),'unused')
except RuntimeError as error:assert 'unsafe group' in str(error)
else:raise AssertionError('helper accepted its own group for shutdown')
print(json.dumps({'command':sys.argv[1:],'pid':os.getpid(),'pgid':os.getpgrp(),'sid':os.getsid(0),'broker':'absent','reads':'validated','self_signal':'refused'}))
