#!/usr/bin/env python3
"""Executed-path regressions; every service/process observation is fake."""
import sys
sys.dont_write_bytecode=True
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import plistlib
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
from testdata.rolling_upgrade_gui_fixture import gui_output,empty_services

spec=importlib.util.spec_from_file_location('r3_safety',os.environ.get('B1_SAFETY',str(Path(__file__).with_name('rolling_upgrade_safety.py'))))
s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)

def runner():
    spec=importlib.util.spec_from_file_location('r3_runner',os.environ.get('B1_RUNNER',str(Path(__file__).with_name('measure-rolling-upgrade.py'))))
    result=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'rolling_upgrade_safety':s}):spec.loader.exec_module(result)
    return result

class Round3(unittest.TestCase):
    def setUp(self):
        for name,value in [('system_launchctl',Mock(side_effect=AssertionError('real launchctl forbidden'))),('process_rows',Mock(return_value=[]))]:
            p=patch.object(s,name,value);p.start();self.addCleanup(p.stop)

    def managed(self,tmp,identity='a'*32):
        parent=Path(tmp).resolve();root=parent/('wefty-b1-proof-'+identity)
        m=s.Manifest.create(parent/('wefty-b1-evidence-'+identity)/'manifest.json',root,['127.0.0.1:59101','127.0.0.1:59102'])
        m.mkdir(root)
        return m

    def service(self,m):
        d=m.read();home=Path(d['root'])/'home';label=d['prefix']+'l1'
        definition={'Label':label,'ProgramArguments':[str(home/'.local/bin/wefty-l1')],
          'WorkingDirectory':str(home/'data'),'StandardOutPath':str(home/'log'),'StandardErrorPath':str(home/'log'),
          'EnvironmentVariables':{'B1_PROOF_ID':d['id'],'HOME':str(home),'TMPDIR':str(home/'tmp'),'GOCACHE':str(home/'cache'),'GOMODCACHE':str(home/'mod'),'GOPATH':str(home/'go'),'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB','GOFLAGS':'-p=1'}}
        path=home/'Library/LaunchAgents'/(label+'.plist');path.parent.mkdir(parents=True);path.write_bytes(plistlib.dumps(definition))
        with m.locked() as d:d['labels'][label]={'initial_absent':True,'mutated':True,'definition':definition}
        return label,path,definition

    def fake_service(self,label,path,definition):
        loaded={label};calls=[]
        def fake(args):
            calls.append(args)
            if args==['print','gui/'+str(os.getuid())]:return 0,gui_output(sorted(loaded))
            if args[0]=='bootout':loaded.discard(args[1].split('/',2)[2]);return 0,b''
            if args[1].split('/',2)[2] not in loaded:return 113,b'absent'
            return 0,('path = '+str(path)+'\nprogram = '+definition['ProgramArguments'][0]+'\n').encode()
        return loaded,calls,fake

    def test_evidence_a_cannot_redirect_to_live_run_b(self):
        with tempfile.TemporaryDirectory() as tmp:
            a=self.managed(tmp);b=self.managed(tmp,'b'*32);sentinel=Path(b.read()['root'])/'protected';sentinel.write_text('B')
            label,path,definition=self.service(b)
            loaded,calls,service=self.fake_service(label,path,definition)
            data=b.read();data['evidence']=str(a.path.parent);s.atomic_json(a.path,data)
            fresh=s.Manifest(a.path);fake=Mock(side_effect=service);stop=Mock();refused=False
            try:s.teardown(fresh,invoke=fake,stop=stop)
            except RuntimeError:refused=True
            self.assertTrue(refused,'evidence A accepted B identity')
            self.assertTrue(sentinel.exists(),'A deleted B runtime')
            fake.assert_not_called();stop.assert_not_called();self.assertEqual(loaded,{label})
            with self.assertRaises(RuntimeError):fresh.open_root()
            with self.assertRaises(RuntimeError):
                with fresh.controller():pass

    def test_teardown_reloads_label_published_while_joining_controller(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);original=m.controller
            label,path,definition=self.service(m)
            loaded,calls,fake=self.fake_service(label,path,definition);loaded.clear()
            with m.locked() as d:d['labels']={};d['validated']=[0,1,2]
            target='gui/'+str(os.getuid())+'/'+label
            def completing_broker(args):
                if args[0]=='kickstart':loaded.add(label);return 0,b''
                return fake(args)
            @contextlib.contextmanager
            def join(*args,**kwargs):
                # The already-started manual operation finishes through the
                # real allowlist/reservation/journal path before releasing its
                # lifecycle lease. Teardown must read its final publication.
                s.launchctl(m,['kickstart',target],invoke=completing_broker)
                with original(*args,**kwargs):yield
            with patch.object(m,'controller',side_effect=join):
                try:s.teardown(m,invoke=fake)
                except RuntimeError:pass
            self.assertEqual(loaded,set(),'label registered during join survived teardown')
            self.assertIn(['bootout',target],calls)

    def test_omitted_inventory_never_permits_loaded_namespace_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);label,path,definition=self.service(m)
            with m.locked() as d:d['labels']={}
            loaded,calls,fake=self.fake_service(label,path,definition)
            try:s.teardown(m,invoke=fake)
            except RuntimeError:pass
            self.assertFalse(loaded and m.read()['teardown']=='absent','omitted label became a false absence verdict')
            self.assertFalse(loaded and not Path(m.read()['root']).exists(),'loaded namespace lost its runtime')
            self.assertIn(['print','gui/'+str(os.getuid())],calls,'exact live namespace was never reconciled')

    def saved_session(self,m):
        with m.locked() as d:d['groups']['12345']={'birth':'old','kind':'command','active':False,'sid':12345}
        return s.Manifest(m.path)

    def test_restart_saved_session_blocks_teardown_without_signalling(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);fresh=self.saved_session(m);root=Path(m.read()['root']);stop=Mock()
            with patch.object(s,'process_rows',return_value=[(os.getuid(),12346,1,12346)]),patch.object(s.os,'getsid',return_value=12345),patch.object(s.os,'killpg',side_effect=ProcessLookupError()):
                try:s.teardown(fresh,invoke=empty_services,stop=stop)
                except RuntimeError:pass
            self.assertTrue(root.exists(),'fresh recovery deleted files despite saved-session survivor')
            self.assertNotEqual(fresh.read()['teardown'],'absent');stop.assert_not_called()

    def test_restart_unavailable_session_inspection_blocks_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);fresh=self.saved_session(m);root=Path(m.read()['root']);stop=Mock()
            with patch.object(s,'process_rows',return_value=[(os.getuid(),12346,1,12346)]),patch.object(s.os,'getsid',side_effect=PermissionError()),patch.object(s.os,'killpg',side_effect=ProcessLookupError()):
                try:s.teardown(fresh,invoke=empty_services,stop=stop)
                except RuntimeError:pass
            self.assertTrue(root.exists(),'unavailable saved-session inspection was treated as absent')
            self.assertNotEqual(fresh.read()['teardown'],'absent');stop.assert_not_called()

    def test_restart_saved_session_blocks_manual_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            fresh=self.saved_session(self.managed(tmp));refused=False
            with fresh.controller(),patch.object(s,'process_rows',return_value=[(os.getuid(),12346,1,12346)]),patch.object(s.os,'getsid',return_value=12345),patch.object(s.os,'killpg',side_effect=ProcessLookupError()):
                try:fresh.require_idle(fresh.read())
                except RuntimeError:refused=True
            self.assertTrue(refused,'recovery accepted a surviving saved session')

    def test_dry_run_identifies_unreviewed_ref(self):
        b=runner();policy=json.loads(Path(__file__).with_name('testdata').joinpath('rolling-upgrade-source-policy.json').read_text())
        args=types.SimpleNamespace(repo=str(Path(__file__).resolve().parent.parent),refs=['HEAD']*3,evidence_dir=None)
        out=io.StringIO()
        with patch.object(b,'compilation_fingerprint',return_value='unreviewed') as fingerprint,contextlib.redirect_stdout(out):
            try:b.plan(args)
            except (RuntimeError,SystemExit):pass
        self.assertEqual(out.getvalue().count('compilation policy REFUSE'),3,'source refusal missing; unrelated readiness refusal is insufficient')
        self.assertEqual(fingerprint.call_count,1,'reviewed archive fingerprint was not checked')

    def test_single_run_lock_and_stale_namespace_refuse_new_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent=Path(tmp).resolve()
            with s.trial_lock(parent):
                with self.assertRaisesRegex(RuntimeError,'single-run lock'):
                    with s.trial_lock(parent):self.fail('second run entered')
            root=parent/('wefty-b1-proof-'+'f'*32);root.mkdir()
            fake=Mock(return_value=(0,gui_output()))
            with self.assertRaisesRegex(RuntimeError,'proof root'):s.require_empty_namespace(parent,fake)
            fake.assert_not_called();root.rmdir()
            with self.assertRaisesRegex(RuntimeError,'proof label'):
                s.require_empty_namespace(parent,lambda args:(0,gui_output([s.PREFIX+'f'*32+'.l1'])))

    def test_every_spawn_persists_new_session_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);child=Mock(pid=12345)
            with m.controller(),patch.object(s.subprocess,'Popen',return_value=child) as spawn,patch.object(s,'process_birth',return_value='birth'):
                m.spawn(['fake-command'])
            self.assertTrue(spawn.call_args.kwargs['start_new_session'])
            self.assertEqual(s.Manifest(m.path).read()['groups']['12345']['sid'],12345)
            def fake_broker(args,**kwargs):
                kwargs['journal'](12346,'broker-birth',True)
                kwargs['journal'](12346,'broker-birth',False)
                return 0,gui_output()
            with patch.object(s,'system_launchctl',side_effect=fake_broker):s.launchctl(m,['print','gui/'+str(os.getuid())])
            self.assertEqual(s.Manifest(m.path).read()['groups']['12346']['sid'],12346)

    def test_dry_run_from_subdirectory_checks_full_repository(self):
        b=runner();args=types.SimpleNamespace(repo=str(Path(__file__).resolve().parent),refs=None)
        import tarfile
        def root_fingerprint(archive):
            with tarfile.open(fileobj=io.BytesIO(archive)) as tree:names=tree.getnames()
            return b.source_policy()['compilation_fingerprint'] if 'go.mod' in names else 'wrong-subtree'
        with patch.object(b,'compilation_fingerprint',side_effect=root_fingerprint):repo,refs=b.resolved_refs(args)
        self.assertEqual(repo,Path(__file__).resolve().parent.parent)
        self.assertTrue(all(accepted for _,_,accepted in refs),'git archive used only a subtree')

    def test_restart_confirmed_session_disappearance_permits_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);fresh=self.saved_session(m);stop=Mock()
            with patch.object(s,'process_rows',return_value=[(os.getuid(),12346,1,12346)]),patch.object(s.os,'getsid',side_effect=ProcessLookupError()),patch.object(s.os,'killpg',side_effect=ProcessLookupError()):
                s.teardown(fresh,invoke=empty_services,stop=stop)
            self.assertFalse(Path(m.read()['root']).exists())
            self.assertEqual(fresh.read()['teardown'],'absent');stop.assert_not_called()

    def test_default_mode_only_calls_plan_with_all_effects_stubbed(self):
        b=runner()
        with patch.object(sys,'argv',['runner']),patch.object(b,'plan',return_value=(True,None,None,None,None)) as plan,patch.object(b,'Proof',side_effect=AssertionError('execution forbidden')):
            self.assertEqual(b.main(),0)
        plan.assert_called_once()

    def test_unavailable_namespace_never_reports_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp)
            with self.assertRaisesRegex(RuntimeError,'namespace'):
                s.teardown(m,invoke=lambda args:(0,b'unverified output') if args==['print','gui/'+str(os.getuid())] else (113,b''))
            self.assertTrue(Path(m.read()['root']).exists())
            self.assertEqual(m.read()['teardown'],'incomplete')

    def test_manual_teardown_stop_byte_failure_still_unloads(self):
        b=runner()
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);label,path,definition=self.service(m)
            loaded,calls,fake=self.fake_service(label,path,definition)
            with patch.object(sys,'argv',['runner','--teardown',str(m.path.parent)]),patch.object(s.os,'pwrite',side_effect=OSError('ENOSPC control')),patch.object(s,'system_launchctl',side_effect=lambda args,**kwargs:fake(args)):
                try:b.main()
                except (RuntimeError,OSError):pass
            self.assertFalse(loaded,'manual control-byte persistence failure skipped all shutdown')
            self.assertTrue(Path(m.read()['root']).exists(),'incomplete control/evidence must retain files')

if __name__=='__main__':unittest.main()
