"""Test child only: real controller/signals/teardown, fake service manager."""
import sys
sys.dont_write_bytecode=True
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import time
from unittest.mock import patch

scripts=Path(__file__).resolve().parents[1];sys.path.insert(0,str(scripts))
spec=importlib.util.spec_from_file_location('b1_signal',os.environ.get('B1_RUNNER',str(scripts/'measure-rolling-upgrade.py')))
b1=importlib.util.module_from_spec(spec);spec.loader.exec_module(b1)
import rolling_upgrade_safety as safety
from testdata.rolling_upgrade_gui_fixture import gui_output

class Fixture:
    manifest=None
    def initialize(self):
        base=Path(sys.argv[1]).resolve();root=base/('wefty-b1-proof-'+'d'*32)
        self.manifest=safety.Manifest.create(base/('wefty-b1-evidence-'+'d'*32)/'manifest.json',root,['127.0.0.1:59101','127.0.0.1:59102'])
        self.lifecycle=self.manifest.controller();self.lifecycle.__enter__();self.manifest.mkdir(root)
        self.root=root;home=root/'home';self.label=self.manifest.read()['prefix']+'l1'
        self.path=home/'Library/LaunchAgents'/(self.label+'.plist');self.path.parent.mkdir(parents=True)
        self.definition={'Label':self.label,'ProgramArguments':[str(home/'.local/bin/wefty-l1'),'--profile='+str(home/'.config/wefty/profile.json')],
          'WorkingDirectory':str(home/'data'),'StandardOutPath':str(home/'logs/l1.log'),'StandardErrorPath':str(home/'logs/l1.log'),
          'EnvironmentVariables':{'B1_PROOF_ID':'d'*32,'HOME':str(home),'TMPDIR':str(home/'tmp'),'GOCACHE':str(home/'cache'),'GOMODCACHE':str(home/'mod'),'GOPATH':str(home/'go'),'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB','GOFLAGS':'-p=1'}}
        self.path.write_bytes(plistlib.dumps(self.definition))
        with self.manifest.locked() as d:d['labels'][self.label]={'initial_absent':True,'mutated':True,'definition':self.definition}
        self.loaded=True
    def execute(self):
        print('READY',flush=True)
        while True:time.sleep(.02)
    def record(self,*args):pass
    def teardown(self):
        def fake(args):
            if args==['print','gui/'+str(os.getuid())]:return 0,gui_output([self.label] if self.loaded else [])
            if args[0]=='bootout':
                print('CLEANUP_READY',flush=True);time.sleep(.3);self.loaded=False;return 0,b''
            if not self.loaded:return 113,b'absent'
            return 0,('path = '+str(self.path)+'\nprogram = '+self.definition['ProgramArguments'][0]+'\n').encode()
        with patch.object(safety,'process_rows',return_value=[]),patch.object(safety,'system_launchctl',side_effect=AssertionError('forbidden')):
            safety.teardown(self.manifest,invoke=fake)
        print('CLEANED',flush=True)

b1.controlled_trial(Fixture())
