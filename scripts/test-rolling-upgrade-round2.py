#!/usr/bin/env python3
"""Behavioral safety regressions. No real service manager or installation."""
import sys
sys.dont_write_bytecode = True
import importlib.util
import json
import plistlib
import io
import signal
import subprocess
import threading
import types
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from testdata.rolling_upgrade_gui_fixture import gui_output,empty_services

path=os.environ.get('B1_SAFETY',str(Path(__file__).with_name('rolling_upgrade_safety.py')))
spec=importlib.util.spec_from_file_location('r2_safety',path)
s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)

class Round2(unittest.TestCase):
    def setUp(self):
        p=patch.object(s,'system_launchctl',side_effect=AssertionError('real launchctl forbidden'))
        p.start();self.addCleanup(p.stop)
        p=patch.object(s,'process_rows',return_value=[]);p.start();self.addCleanup(p.stop)
        p=patch.object(s.os,'getsid',return_value=0);p.start();self.addCleanup(p.stop)

    def managed(self,tmp):
        base=Path(tmp).resolve();root=base/('wefty-b1-proof-'+'e'*32)
        m=s.Manifest.create(base/('wefty-b1-evidence-'+'e'*32)/'manifest.json',root,['127.0.0.1:59101','127.0.0.1:59102'])
        m.mkdir(root)
        return m

    def reused_pid_fixture(self,tmp,fresh=False):
        m=self.managed(tmp)
        with m.controller():m.group(59802,'trial-birth')
        return s.Manifest(m.path) if fresh else m

    def test_run5_reused_pid_in_an_unrelated_group_is_absent_without_signalling(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.reused_pid_fixture(tmp);stop=Mock()
            rows=[(os.getuid(),59802,1,61000)]
            with patch.object(s,'process_rows',return_value=rows),patch.object(s,'process_birth',return_value='later-birth'),patch.object(s.os,'getsid',return_value=61000),patch.object(s.os,'killpg',side_effect=ProcessLookupError()),patch.object(s.os,'kill') as kill:
                try:s.teardown(m,invoke=Mock(side_effect=empty_services),stop=stop)
                except RuntimeError:pass
            self.assertEqual(m.read()['teardown'],'absent','confirmed PID reuse blocked teardown')
            self.assertFalse(Path(m.read()['root']).exists());stop.assert_not_called();kill.assert_not_called()
            events=[json.loads(line).get('pid_reused') for line in m.path.with_suffix('.ownership').read_text().splitlines()]
            self.assertIn({'pid':59802,'saved_birth':'trial-birth','observed_birth':'later-birth'},events)

    def test_restarted_cleanup_excludes_reused_saved_session_and_group_member(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.reused_pid_fixture(tmp,fresh=True);stop=Mock()
            rows=[(os.getuid(),59802,1,59802)]
            with patch.object(s,'process_rows',return_value=rows),patch.object(s,'process_birth',return_value='later-birth'),patch.object(s.os,'getsid',return_value=59802),patch.object(s.os,'killpg') as killpg,patch.object(s.os,'kill') as kill:
                try:s.teardown(m,invoke=Mock(side_effect=empty_services),stop=stop)
                except RuntimeError:pass
            self.assertEqual(m.read()['teardown'],'absent','reused saved identity was mistaken for an original session/group survivor')
            self.assertFalse(Path(m.read()['root']).exists());stop.assert_not_called();kill.assert_not_called()
            self.assertTrue(all(call.args[1]==0 for call in killpg.call_args_list))
            self.assertEqual(m.read()['pid_reused']['pid'],59802)

    def test_unavailable_birth_and_other_orphan_members_still_block(self):
        for fresh,birth,orphan,sid in ((False,'',False,59802),(True,'',False,59802),
                                     (True,'',False,61000),(True,'later-birth',True,59802)):
            with self.subTest(fresh=fresh,birth=birth,orphan=orphan,sid=sid),tempfile.TemporaryDirectory() as tmp:
                m=self.reused_pid_fixture(tmp,fresh=fresh);stop=Mock()
                rows=[(os.getuid(),59802,1,59802)]+([(os.getuid(),59803,1,59802)] if orphan else [])
                with patch.object(s,'process_rows',return_value=rows),patch.object(s,'process_birth',return_value=birth),patch.object(s.os,'getsid',return_value=sid),patch.object(s.os,'killpg'),patch.object(s.os,'kill'):
                    with self.assertRaises(RuntimeError):s.teardown(m,invoke=Mock(side_effect=empty_services),stop=stop)
                self.assertEqual(m.read()['teardown'],'incomplete');self.assertTrue(Path(m.read()['root']).exists());stop.assert_not_called()

    def test_manual_idle_excludes_reused_saved_identity_but_blocks_unknown(self):
        for birth in ('later-birth',''):
            with self.subTest(birth=birth),tempfile.TemporaryDirectory() as tmp:
                m=self.reused_pid_fixture(tmp,fresh=True)
                with m.controller(),patch.object(s,'process_rows',return_value=[(os.getuid(),59802,1,59802)]),patch.object(s,'process_birth',return_value=birth),patch.object(s.os,'getsid',return_value=59802),patch.object(s.os,'killpg') as killpg:
                    error=None
                    try:m.require_idle(m.read())
                    except RuntimeError as caught:error=str(caught)
                if birth:self.assertIsNone(error,'confirmed reuse blocked recovery')
                else:self.assertIsNotNone(error,'unknown identity allowed recovery')
                self.assertTrue(all(call.args[1]==0 for call in killpg.call_args_list))

    def service(self,m):
        d=m.read();home=Path(d['root'])/'home';label=d['prefix']+'l1'
        definition={'Label':label,'ProgramArguments':[str(home/'.local/bin/wefty-l1'),'--profile='+str(home/'.config/wefty/profile.json')],
                    'WorkingDirectory':str(home/'data'),'StandardOutPath':str(home/'logs/l1.log'),'StandardErrorPath':str(home/'logs/l1.log'),
                    'EnvironmentVariables':{'B1_PROOF_ID':d['id'],'HOME':str(home),'TMPDIR':str(home/'tmp'),'GOCACHE':str(home/'cache'),'GOMODCACHE':str(home/'mod'),'GOPATH':str(home/'go'),'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB','GOFLAGS':'-p=1'}}
        path=home/'Library/LaunchAgents'/(label+'.plist');path.parent.mkdir(parents=True);path.write_bytes(plistlib.dumps(definition))
        with m.locked() as current:current['labels'][label]={'initial_absent':True,'mutated':True,'definition':definition}
        loaded={label};calls=[]
        def fake(args):
            calls.append(args)
            if args==['print','gui/'+str(os.getuid())]:return 0,gui_output(sorted(loaded))
            if args[0]=='bootout':loaded.clear();return 0,b''
            if not loaded:return 113,b'absent'
            return 0,('path = '+str(path)+'\nprogram = '+definition['ProgramArguments'][0]+'\n').encode()
        return loaded,calls,fake

    def test_manifest_daily_path_sentinel_is_never_a_deletion_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);daily=Path(tmp).resolve()/'daily-wefty';daily.write_text('protected')
            data=json.loads(m.path.read_text());st=daily.stat()
            data['paths'][str(daily)]={'kind':'file','identity':[st.st_dev,st.st_ino]}
            s.atomic_json(m.path,data)
            fake=Mock(side_effect=empty_services)
            refused=False
            with patch.object(s,'discover_groups',return_value=0,create=True):
                try:s.teardown(m,invoke=fake,stop=Mock())
                except RuntimeError:refused=True
            self.assertTrue(daily.exists(),'manifest authorized deletion outside the proof root')
            self.assertTrue(refused,'invalid complete manifest was not rejected')
            fake.assert_not_called()
            self.assertEqual(daily.read_text(),'protected')

    def test_journal_failure_does_not_prevent_owned_shutdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);loaded,calls,fake=self.service(m)
            with patch.object(s,'atomic_json',side_effect=OSError('ENOSPC')),patch.object(s,'discover_groups',return_value=0,create=True):
                try:s.teardown(m,invoke=fake,stop=Mock())
                except (RuntimeError,OSError):pass
            self.assertEqual(loaded,set(),'journal failure prevented all shutdown')
            self.assertTrue(Path(m.cache['root']).exists(),'evidence failure must retain runtime files')

    def test_unrelated_inspector_cannot_authorize_its_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);d=m.read()
            if hasattr(m,'refresh_owned'):
                with patch.object(s,'process_rows',return_value=[(os.getuid(),12345,12345)]):m.refresh_owned()
            else:
                args={'executable':'/usr/bin/readlink','argv':['readlink',d['root']+'/tmp/alias'],'environment':{}}
                with patch.object(s,'process_rows',return_value=[(os.getuid(),12345,12345),(os.getuid(),12346,12345)]),patch.object(s,'process_birth',return_value='birth'),patch.object(s,'process_arguments',return_value=args):
                    s.discover_groups(m,time.monotonic()+10)
            self.assertEqual(m.read()['groups'],{},'path inspector claimed an unrelated process group')

    def test_descriptor_relative_cleanup_preserves_external_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);root=Path(m.read()['root'])
            daily=Path(tmp)/'daily';daily.write_text('protected');daily.chmod(0o400)
            (root/'link').symlink_to(daily)
            os.link(daily,root/'hardlink')
            before=daily.stat().st_mode
            real_unlink=os.unlink;calls=[]
            def unlink(name,**kwargs):calls.append((name,kwargs));return real_unlink(name,**kwargs)
            with patch.object(s.os,'unlink',side_effect=unlink):s.teardown(m,invoke=Mock(side_effect=empty_services))
            self.assertFalse(root.exists());self.assertEqual(daily.read_text(),'protected')
            self.assertEqual(daily.stat().st_mode,before)
            self.assertTrue(calls);self.assertTrue(all('dir_fd' in kw and not Path(name).is_absolute() for name,kw in calls))

    def test_stale_root_and_foreign_label_refuse_before_effects(self):
        for field in ('root_identity','labels'):
            with self.subTest(field=field),tempfile.TemporaryDirectory() as tmp:
                m=self.managed(tmp);d=m.read()
                if field=='root_identity':d[field]=[0,0]
                else:d[field]={'dev.wefty.l1':{'initial_absent':True,'mutated':True}}
                s.atomic_json(m.path,d);fake=Mock()
                with self.assertRaises(RuntimeError):s.teardown(m,invoke=fake)
                fake.assert_not_called()
                self.assertTrue(Path(d['root']).exists())

    def test_mixed_group_is_not_signalled_and_escalation_rechecks_birth(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp)
            m.children[12345]={'birth':'born','pgid':12345,'source':'spawn'}
            rows=[(os.getuid(),12345,1,12345),(os.getuid(),12346,1,12345)]
            with patch.object(s,'process_rows',return_value=rows),patch.object(s,'process_birth',return_value='born'):
                groups=m.refresh_owned()
                self.assertEqual(set(groups),{12345});self.assertIn(12345,m.ambiguous_groups)
                signals=[]
                def verify():
                    m.refresh_owned()
                    s.require(12345 not in m.ambiguous_groups,'mixed group')
                with patch.object(s.os,'killpg',side_effect=lambda pid,sig:signals.append(sig)):
                    with self.assertRaisesRegex(RuntimeError,'mixed group'):s.stop_group(12345,verify=verify,timeout=0)
                self.assertEqual(signals,[0])
        signals=[];alive=True
        def killpg(pid,sig):
            nonlocal alive
            if not alive:raise ProcessLookupError()
            if sig:signals.append(sig)
            if sig==signal.SIGKILL:alive=False
        with patch.object(s.os,'killpg',side_effect=killpg),patch.object(s.os,'getpgid',return_value=12345),patch.object(s,'process_birth',side_effect=['born','reused']):
            with self.assertRaisesRegex(RuntimeError,'identity'):s.stop_group(12345,'born',timeout=0)
        self.assertEqual(signals,[signal.SIGTERM],'escalation signalled a reused PID')

    def test_unknown_owned_candidate_blocks_absence_but_other_owned_work_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);root=Path(m.read()['root'])
            m.children={10001:{'birth':'a','pgid':10001,'source':'spawn'},10002:{'birth':'b','pgid':10002,'source':'spawn'}}
            m.owned={10001:'a',10002:'b'}
            rows=[(os.getuid(),10001,1,10001),(os.getuid(),10002,1,10002),(os.getuid(),10003,1,10003)]
            stopped=[]
            def stop(group,**kwargs):kwargs['verify']();stopped.append(group);rows[:]=[r for r in rows if r[1]!=group]
            with patch.object(s,'process_rows',side_effect=lambda:list(rows)),patch.object(s,'process_birth',side_effect=lambda pid:'' if pid==10001 else 'b'),patch.object(s.os,'kill',return_value=None),patch.object(s.os,'getpgid',side_effect=lambda pid:pid):
                with self.assertRaisesRegex(RuntimeError,'survivors'):s.teardown(m,invoke=Mock(side_effect=empty_services),stop=stop)
            self.assertEqual(stopped,[10002]);self.assertTrue(root.exists())
            self.assertNotEqual(m.read()['teardown'],'absent')

    def test_closed_records_are_preserved_without_following_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);root=Path(m.read()['root'])
            folder=root/'home/.config/wefty';folder.mkdir(parents=True)
            (folder/'proof.json').write_text('{"proof":true}')
            daily=Path(tmp).resolve()/'daily.json';daily.write_text('{"private":true}')
            (folder/'external.json').symlink_to(daily)
            s.teardown(m,invoke=Mock(side_effect=empty_services))
            records=list(m.path.parent.glob('closed-records-*/home/.config/wefty/proof.json'))
            self.assertEqual(len(records),1);self.assertEqual(records[0].read_text(),'{"proof":true}')
            self.assertFalse(list(m.path.parent.glob('closed-records-*/home/.config/wefty/external.json')))
            self.assertTrue(daily.exists());self.assertFalse(root.exists())

    def test_record_capture_failure_happens_after_service_shutdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);loaded,calls,fake=self.service(m)
            with patch.object(s,'preserve_closed_records',side_effect=OSError('ENOSPC capture')):
                with self.assertRaisesRegex(RuntimeError,'ENOSPC capture'):s.teardown(m,invoke=fake)
            self.assertFalse(loaded);self.assertTrue(Path(m.read()['root']).exists())

    def test_read_is_read_only_and_each_journal_failure_still_unloads(self):
        for failure in ('append','fsync','replace','read','control'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as tmp:
                m=self.managed(tmp);loaded,calls,fake=self.service(m);root=Path(m.read()['root'])
                with patch.object(s,'atomic_json',side_effect=AssertionError('read wrote journal')):m.read()
                real_open=s.os.open;real_read=s.Path.read_text
                def opened(path,*args,**kwargs):
                    if str(path).endswith('.ownership'):raise OSError('ENOSPC append')
                    return real_open(path,*args,**kwargs)
                def read(path,*args,**kwargs):
                    if path==m.path:raise OSError('EACCES read')
                    return real_read(path,*args,**kwargs)
                manager={'append':patch.object(s.os,'open',side_effect=opened),'fsync':patch.object(s.os,'fsync',side_effect=OSError('ENOSPC sync')),
                         'replace':patch.object(s.os,'replace',side_effect=OSError('ENOSPC replace')),'read':patch.object(s.Path,'read_text',read),'control':patch.object(s.os,'pwrite',side_effect=OSError('ENOSPC control'))}[failure]
                with manager:
                    with self.assertRaises((RuntimeError,OSError)):s.teardown(m,invoke=fake)
                self.assertFalse(loaded,'persistence failure skipped shutdown');self.assertTrue(root.exists())

    def test_lifecycle_stop_joins_controller_and_prevents_late_spawn(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);entered=threading.Event();released=threading.Event();spawned=[]
            def controller():
                with m.controller():
                    entered.set()
                    while os.pread(m.controller_fd,1,0)==b'R':time.sleep(.005)
                    with self.assertRaisesRegex(RuntimeError,'closing'):
                        with patch.object(s.subprocess,'Popen',side_effect=lambda *a,**k:spawned.append(a)):m.spawn(['forbidden'])
                    released.set()
            worker=threading.Thread(target=controller);worker.start();self.assertTrue(entered.wait(2))
            other=s.Manifest(m.path)
            s.teardown(other,invoke=Mock(side_effect=empty_services))
            worker.join(2);self.assertFalse(worker.is_alive());self.assertTrue(released.is_set());self.assertFalse(spawned)
            with self.assertRaisesRegex(RuntimeError,'closing'):
                with s.Manifest(m.path).controller():self.fail('manual recovery obtained a closing lease')


    def runner(self):
        spec=importlib.util.spec_from_file_location('r2_runner',os.environ.get('B1_RUNNER',str(Path(__file__).with_name('measure-rolling-upgrade.py'))))
        module=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,{'rolling_upgrade_safety':s}):spec.loader.exec_module(module)
        return module

    def test_manual_cancel_failure_and_real_refusal_response_are_visible(self):
        b1=self.runner()
        production=(Path(__file__).resolve().parents[1]/'cmd/wefty/upgrade_rolling_recovery.go').read_text()
        import re
        message=re.search(r'fmt.Sprintf\("(destructive rolling rollback refused:[^"]+)"',production)[1].replace('%d','1')
        for args,code,reason in ((['cancel','run-id'],1,'cancellation failed'),(['upgrade','--rolling'],5,message)):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as tmp:
                m=self.managed(tmp);d=m.read();home=Path(d['root'])/'home'
                profile=home/'.config/wefty/profile.json';profile.parent.mkdir(parents=True)
                profile.write_text(json.dumps({'fabric':'plain','l1_address':d['ports'][0],'l3_address':d['ports'][1]}))
                binary=home/'.local/bin/wefty';binary.parent.mkdir(parents=True)
                import shlex
                binary.write_text('#!/bin/sh\nprintf %s '+shlex.quote(reason)+' >&2\nexit '+str(code)+'\n');binary.chmod(0o700)
                out=io.StringIO();err=io.StringIO()
                with patch.object(b1.Proof,'guard'),patch.object(b1.Proof,'disk_guard'),patch.object(s,'process_birth',return_value='birth'),patch.object(b1,'process_birth',return_value='birth'),patch('rolling_upgrade_safety.process_birth',return_value='birth'),patch('sys.argv',['runner','--proof-command',str(m.path.parent),'--']+args),patch('sys.stdout',out),patch('sys.stderr',err):
                    result=b1.main()
                self.assertEqual(result,code,'manual wrapper hid child failure')
                self.assertIn(reason,err.getvalue(),'manual wrapper hid stderr')
                self.assertEqual(out.getvalue(),'')
                self.assertTrue(any(reason in p.read_text() for p in m.path.parent.glob('*.err')))

    def test_proof_exec_adapter_cannot_spawn_detached_cleanup_helpers(self):
        b1=self.runner();base=Path(__file__).resolve().parents[1]
        wrapper,cleanup=b1.patch_exec_aliases((base/'cmd/wefty/exec_wrapper.sh').read_text(),(base/'internal/execsweep/exec_cleanup.sh').read_text(),Path('/proof/tmp/wefty-run.'))
        self.assertTrue(wrapper.startswith('#!/bin/bash\n'))
        self.assertIn('/proof/tmp/wefty-run.',wrapper)
        self.assertIn('tmp_root=/proof/tmp',wrapper)
        self.assertNotIn('cd -- /var/tmp',wrapper)
        self.assertIn('$tmp_root/wefty-run-data.XXXXXXXX',wrapper)
        self.assertEqual(cleanup.splitlines()[-1],'retire_exec_storage() { return 0; }')
        self.assertNotIn('set -m',cleanup)

    def test_termination_during_spawn_waits_for_creation_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);child=Mock(pid=12345);child.poll.return_value=None
            def spawn(*args,**kwargs):
                signal.getsignal(signal.SIGINT)(signal.SIGINT,None)
                return child
            with m.controller(),patch.object(s.subprocess,'Popen',side_effect=spawn),patch.object(s,'process_birth',return_value='birth'),patch.object(s,'stop_group') as stop:
                with self.assertRaises(KeyboardInterrupt):m.spawn(['fake-wefty'])
            self.assertIn(12345,m.owned);stop.assert_called_once()

    def test_spawn_registration_survives_teardown_during_popen(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);child=Mock(pid=12345);child.poll.return_value=None
            def spawn(*args,**kwargs):
                self.assertTrue(kwargs['start_new_session'])
                os.pwrite(m.controller_fd,b'X',0)
                return child
            with m.controller(),patch.object(s.subprocess,'Popen',side_effect=spawn),patch.object(s,'process_birth',return_value='birth'),patch.object(s,'stop_group') as stop:
                with self.assertRaisesRegex(RuntimeError,'closing'):m.spawn(['fake-wefty'])
            self.assertIn(12345,m.owned);stop.assert_called_once()

    def test_second_ctrl_c_during_actual_cleanup_is_ignored(self):
        import select
        fixture=Path(__file__).with_name('testdata')/'rolling_upgrade_signal_fixture.py'
        with tempfile.TemporaryDirectory() as tmp:
            child=subprocess.Popen([sys.executable,str(fixture),tmp],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
            try:
                def marker(expected):
                    self.assertTrue(select.select([child.stdout],[],[],5)[0],'fixture marker timeout')
                    line=child.stdout.readline().strip()
                    if not line:self.fail(child.stderr.read())
                    self.assertEqual(line,expected)
                marker('READY');os.kill(child.pid,signal.SIGINT)
                marker('CLEANUP_READY');os.kill(child.pid,signal.SIGINT)
                out,err=child.communicate(timeout=5)
                self.assertEqual(child.returncode,130,err);self.assertIn('CLEANED',out)
                self.assertFalse((Path(tmp)/('wefty-b1-proof-'+'d'*32)).exists())
                self.assertEqual(json.loads((Path(tmp)/('wefty-b1-evidence-'+'d'*32)/'manifest.json').read_text())['teardown'],'absent')
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=2)
                child.stdout.close();child.stderr.close()


    def test_orphaned_session_member_blocks_absence_without_adoption(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);m.owned[12345]='born'
            rows=[(os.getuid(),12346,1,12346)]
            with patch.object(s,'process_rows',return_value=rows),patch.object(s.os,'getsid',return_value=12345),patch.object(s.os,'killpg',side_effect=ProcessLookupError()):
                m.refresh_owned()
                self.assertEqual(m.uncertain,[12346],'in-memory session candidate was silently skipped')
                with self.assertRaisesRegex(RuntimeError,'survivors'):s.teardown(m,invoke=Mock(side_effect=empty_services))
            self.assertEqual(m.uncertain,[12346]);self.assertNotIn(12346,m.children)
            self.assertTrue(Path(m.read()['root']).exists())

    def test_exited_spawn_leader_cannot_hide_a_surviving_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);m.owned[12345]='born'
            m.children[12345]={'birth':'born','pgid':12345,'source':'spawn'}
            signals=[]
            with patch.object(s.os,'kill',side_effect=ProcessLookupError()),patch.object(s.os,'killpg',side_effect=lambda pid,sig:signals.append(sig)):
                with self.assertRaisesRegex(RuntimeError,'ownership unverified'):s.teardown(m,invoke=Mock(side_effect=empty_services))
            self.assertEqual(signals,[0]);self.assertTrue(Path(m.read()['root']).exists())

    def test_orphaned_command_group_blocks_manual_recovery_before_spawn(self):
        b1=self.runner()
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);d=m.read();home=Path(d['root'])/'home'
            profile=home/'.config/wefty/profile.json';profile.parent.mkdir(parents=True)
            profile.write_text(json.dumps({'fabric':'plain','l1_address':d['ports'][0],'l3_address':d['ports'][1]}))
            with m.locked() as data:data['groups']['12345']={'birth':'plausible','active':False}
            with patch.object(s.os,'killpg',return_value=None),patch.object(b1.Proof,'run',return_value=subprocess.CompletedProcess([],0,'','')) as run,patch('sys.argv',['runner','--proof-command',str(m.path.parent),'--','upgrade','--rolling']):
                with self.assertRaisesRegex(RuntimeError,'recovery refused'):b1.main()
            run.assert_not_called()

    def test_active_controller_excludes_manual_recovery_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp)
            with m.controller():
                with self.assertRaisesRegex(RuntimeError,'controller has not joined'):
                    with s.Manifest(m.path).controller(timeout=0):self.fail('manual recovery raced active controller')

    def test_command_budget_and_spawn_journal_failure_stop_real_fixture(self):
        b1=self.runner()
        budget=s.Budget(10,max_output=32,max_disk=64,min_free=16,now=lambda:11)
        with self.assertRaisesRegex(RuntimeError,'deadline'):budget.check(0,100)
        budget.now=lambda:0
        for used,free,output in ((65,100,0),(0,1,0),(0,100,33)):
            with self.assertRaises(RuntimeError):budget.check(used,free,output)
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);started=[];real_spawn=subprocess.Popen
            def spawn(*args,**kwargs):
                child=real_spawn(*args,**kwargs);started.append(child);return child
            with m.controller(),patch.object(s.subprocess,'Popen',side_effect=spawn),patch.object(s,'process_birth',return_value='born'),patch.object(s,'atomic_json',side_effect=OSError('ENOSPC after spawn')):
                with self.assertRaises(OSError):m.spawn([sys.executable,'-c','import time;time.sleep(10)'])
            self.assertEqual(len(started),1);self.assertIsNotNone(started[0].poll())
            self.assertIn(started[0].pid,m.owned)
            self.assertTrue(m.persistence_errors)

    def test_live_service_and_saved_process_assertions_are_not_authority(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp);loaded,calls,fake=self.service(m);base_fake=fake
            def live(args):
                code,out=base_fake(args)
                return (code,out+b'pid = 12345\n') if code==0 and args[0]=='print' and args[1].startswith('gui/'+str(os.getuid())+'/') else (code,out)
            with patch.object(s,'process_birth',return_value='born'),patch.object(s,'process_executable',return_value='/daily/wefty-l1'):
                with self.assertRaisesRegex(RuntimeError,'identity unverified'):s.teardown(m,invoke=live)
            self.assertTrue(loaded);self.assertTrue(all(c[0] =='print' for c in calls))
        with tempfile.TemporaryDirectory() as tmp:
            m=self.managed(tmp)
            with m.locked() as d:d['groups']['12345']={'birth':'plausible','active':True}
            signals=[]
            with patch.object(s.os,'killpg',side_effect=lambda pid,sig:signals.append(sig)):
                with self.assertRaisesRegex(RuntimeError,'ownership unverified'):s.teardown(m,invoke=Mock(side_effect=empty_services))
            self.assertEqual(signals,[0],'saved PID assertion caused a signal')


if __name__=='__main__':unittest.main(verbosity=2)
