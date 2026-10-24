#!/usr/bin/env python3
"""Round-4 behavioral evidence. No native launchctl or installed execution."""
import sys
sys.dont_write_bytecode=True
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import tarfile
import types
import unittest
from unittest.mock import Mock,patch
from testdata.rolling_upgrade_source_fixture import FINGERPRINT,source_history

spec=importlib.util.spec_from_file_location('r4_helpers',Path(__file__).with_name('test-rolling-upgrade-round3.py'))
h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
s=h.s
REPO=Path(__file__).resolve().parent.parent
POLICY=json.loads(Path(__file__).with_name('testdata').joinpath('rolling-upgrade-source-policy.json').read_text())

def gui_output(labels=(),domain=None):
    domain=domain or 'gui/'+str(os.getuid())
    return (domain+' = {\n\ttype = gui\n\tservice count = '+str(len(labels))+'\n\tservices = {\n'+''.join('\t\t0 - '+label+'\n' for label in labels)+'\t}\n}\n').encode()

class Round4(unittest.TestCase):
    def setUp(self):
        for name,value in [('system_launchctl',Mock(side_effect=AssertionError('native launchctl forbidden'))),('process_rows',Mock(return_value=[]))]:
            p=patch.object(s,name,value);p.start();self.addCleanup(p.stop)

    def crash_snapshot(self,tmp,fixture,doctor_exit=12,text_exit=12,name='after-crash'):
        b=h.runner();proof=b.Proof.__new__(b.Proof)
        proof.root=Path(tmp).resolve();proof.home=proof.root/'home';proof.home.mkdir()
        proof.profile=proof.home/'profile.json';proof.ports=['127.0.0.1:59101','127.0.0.1:59102']
        proof.profile.write_text(json.dumps(dict(zip(('l1_address','l3_address'),proof.ports))))
        proof.bin=proof.home/'wefty';proof.env={};proof.catalog=Mock();proof.record=Mock()
        proof.probes=fixture['probes'];proof.crash_diagnostics=fixture['expectation']
        def run(argv,label,allowed,**kwargs):
            command=str(argv[-1]);as_json='--json' in argv
            code=(doctor_exit if as_json else text_exit) if command=='doctor' else 12
            b.require(code in allowed,command+' failed, exit '+str(code))
            return json.dumps(fixture[command]) if as_json else 'not ready: expected crash finding\n'
        proof.run=Mock(side_effect=run)
        error=None
        try:proof.snapshot(name)
        except RuntimeError as caught:error=str(caught)
        return proof,error

    def crash_fixture(self):
        return json.loads(Path(__file__).with_name('testdata').joinpath('rolling-upgrade-after-crash.json').read_text())

    def test_crash_snapshot_accepts_findings_with_or_without_cancelled_problem(self):
        for cancelled,state,name in ((True,'lost','after-crash'),(False,'lost','after-crash'),
                                     (False,'interrupted','after-crash-cleanup'),(True,'failed','after-crash-cleanup')):
            with self.subTest(cancelled=cancelled,state=state,name=name),tempfile.TemporaryDirectory() as tmp:
                fixture=self.crash_fixture()
                if state!='lost':
                    fixture=json.loads(json.dumps(fixture).replace('"lost"','"'+state+'"').replace('): lost','): '+state))
                if not cancelled:
                    fixture['status']['reasons']=[r for r in fixture['status']['reasons'] if 'run-cancelled' not in r['detail']]
                    fixture['doctor']['findings']=[r for r in fixture['doctor']['findings'] if 'run-cancelled' not in r['detail']]
                proof,error=self.crash_snapshot(tmp,fixture,name=name)
                self.assertIsNone(error,'expected doctor findings rejected: '+str(error))
                event=[call.args[1] for call in proof.record.call_args_list if call.args[0]==name+'-diagnostics'][0]
                self.assertEqual(event['cancelled_problem_present'],{'status':cancelled,'doctor':cancelled})

    def test_crash_snapshot_refuses_doctor_success_in_json_or_text(self):
        for codes in ((0,0),(0,12),(12,0)):
            with self.subTest(codes=codes),tempfile.TemporaryDirectory() as tmp:
                _,error=self.crash_snapshot(tmp,self.crash_fixture(),*codes)
                self.assertIsNotNone(error,'doctor exit 0 falsely proved the expected crash finding')

    def test_crash_snapshot_refuses_wrong_attempt_run_generation_or_missing_problem(self):
        for report in ('status','doctor'):
            for defect in ('attempt_id','run_id','generation','state','problem'):
                with self.subTest(report=report,defect=defect),tempfile.TemporaryDirectory() as tmp:
                    fixture=self.crash_fixture();d=fixture[report]
                    g=next(g for g in d['generations'] if g['agent_generation_id']=='g-crashed')
                    if defect in ('attempt_id','run_id'):g['attempts'][0][defect]+='-replacement'
                    elif defect=='generation':g['agent_generation_id']='g-other'
                    elif defect=='state':g['attempts'][0]['state']='running'
                    else:
                        key='reasons' if report=='status' else 'findings'
                        d[key]=[p for p in d[key] if 'run-crashed' not in p['detail']]
                    _,error=self.crash_snapshot(tmp,fixture)
                    self.assertIsNotNone(error)
                    self.assertIn('crash diagnostic',error)

    def test_crash_snapshot_refuses_survivor_problems_and_applies_to_later_snapshots(self):
        for report,run_only in (('status',False),('doctor',False),('status',True),('doctor',True)):
            with self.subTest(report=report,run_only=run_only),tempfile.TemporaryDirectory() as tmp:
                fixture=self.crash_fixture();d=fixture[report]
                issue={'detail':'Run run-success: lost' if run_only else 'generation g-survivors: Run run-success (Attempt attempt-success): lost'}
                if report=='status':issue['code']='retiring_attempt_problem';d['reasons'].append(issue)
                else:issue.update(check='retiring_attempt_problem',severity='error');d['findings'].append(issue)
                _,error=self.crash_snapshot(tmp,fixture,name='after-crash-cleanup')
                self.assertIsNotNone(error)
                self.assertIn('surviving probe reported as a problem',error)

    def teardown_poll_fixture(self,tmp,statuses):
        m=h.Round3().managed(tmp)
        label,path,definition=h.Round3().service(m)
        target='gui/'+str(os.getuid())+'/'+label
        booted=False;index=0;clock=[0.0]
        def fake(args):
            nonlocal booted,index
            if args==['print','gui/'+str(os.getuid())]:
                return 0,gui_output([label] if not booted or statuses[-1]!=113 else [])
            if args==['bootout',target]:booted=True;return 0,b'bootout accepted'
            self.assertEqual(args,['print',target])
            if not booted:return 0,('path = '+str(path)+'\nprogram = '+definition['ProgramArguments'][0]+'\n').encode()
            status=statuses[min(index,len(statuses)-1)];index+=1
            return status,(b'absent' if status==113 else b'still exiting '+b'x'*2048)
        return m,target,fake,clock

    def label_observations(self,m):
        return [entry['teardown_observation'] for line in m.path.with_suffix('.ownership').read_text().splitlines()
                if 'teardown_observation' in (entry:=json.loads(line))]

    def test_teardown_waits_for_delayed_owned_label_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            m,target,fake,clock=self.teardown_poll_fixture(tmp,[0,113])
            with patch.object(s.time,'monotonic',side_effect=lambda:clock[0]),patch.object(s.time,'sleep',side_effect=lambda delay:clock.__setitem__(0,clock[0]+delay)):
                try:s.teardown(m,invoke=fake)
                except RuntimeError:pass
            self.assertEqual(m.read()['teardown'],'absent','one loaded response was mistaken for permanent teardown failure')
            self.assertFalse(Path(m.read()['root']).exists())
            events=self.label_observations(m)
            self.assertEqual([(e['action'],e['status']) for e in events],[('print',0),('bootout',0),('print',0),('print',113)])
            self.assertTrue(all(e['target']==target and len(e['output'])<=1024 for e in events))
            self.assertEqual(len({(e['attempt'],e['sequence']) for e in events}),len(events))

    def test_teardown_never_absent_retains_runtime_and_every_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            m,target,fake,clock=self.teardown_poll_fixture(tmp,[0,5,0])
            with patch.object(s.time,'monotonic',side_effect=lambda:clock[0]),patch.object(s.time,'sleep',side_effect=lambda delay:clock.__setitem__(0,clock[0]+delay)):
                with self.assertRaises(RuntimeError):s.teardown(m,invoke=fake,deadline_seconds=3.25)
            self.assertEqual(m.read()['teardown'],'incomplete')
            self.assertTrue(Path(m.read()['root']).exists())
            self.assertLessEqual(clock[0],.25,'polling exceeded the teardown deadline')
            events=self.label_observations(m)
            self.assertTrue(events,'missing per-label teardown evidence')
            self.assertEqual(events[0]['target'],target)
            self.assertEqual([e['status'] for e in events if e['action']=='print'],[0,0,5,0])
            self.assertTrue(all(len(e['output'])<=1024 for e in events))

    def recorded_broker_proof(self,tmp):
        b=h.runner();proof=b.Proof.__new__(b.Proof)
        proof.root=Path(tmp).resolve()/('wefty-b1-proof-'+'c'*32)
        proof.evidence=proof.root.with_name('wefty-b1-evidence-'+'c'*32)
        proof.home=proof.root/'home';proof.profile=proof.home/'.config/wefty/profile.json'
        proof.ports=[];proof.record=Mock();proof.serial=0;proof.env=dict(os.environ)
        proof.budget=s.Budget(__import__('time').monotonic()+30)
        # Execute the actual initializer that writes the broker. Only host
        # inspection, fixed-port reservation and resource guards are faked.
        import shutil
        copy=shutil.copyfile
        def safety_copy(source,destination):return copy(s.__file__,destination)
        with patch.object(b,'require_empty_namespace'),patch.object(b.shutil,'disk_usage',return_value=types.SimpleNamespace(free=100<<30)),patch.object(b.socket,'socket'),patch.object(b.shutil,'copyfile',side_effect=safety_copy):
            proof.initialize()
        self.addCleanup(proof.lifecycle.__exit__,None,None,None)
        # Replace the copied module's native backend, leaving generated broker,
        # Manifest validation/locking/persistence and dispatch unchanged.
        with (proof.evidence/'rolling_upgrade_safety.py').open('a') as out:
            out.write("\n"+Path(__file__).with_name('testdata').joinpath('rolling_upgrade_fake_service.py').read_text())
        proof.env.update(B1_FIXTURE_MANIFEST=str(proof.manifest.path),B1_FIXTURE_BROKER=str(proof.broker),PYTHONDONTWRITEBYTECODE='1')
        proof.guard=Mock();proof.disk_guard=Mock()
        return b,proof

    def test_recorded_commands_invoke_generated_broker_and_read_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            b,proof=self.recorded_broker_proof(tmp)
            fixture=Path(__file__).with_name('testdata')/'rolling_upgrade_recorded_helper.py'
            for command in ('setup','upgrade','status','doctor'):
                with self.subTest(command=command):
                    result=proof.run([sys.executable,fixture,command],'fixture:'+command,guarded=False,allowed=range(-127,256),completed=True)
                    self.assertEqual(result.returncode,0,result.stderr)
                    observed=json.loads(result.stdout)
                    self.assertEqual(observed['reads'],'validated')
                    self.assertEqual(observed['self_signal'],'refused')
                    self.assertNotEqual(observed['pgid'],os.getpgrp())
                    self.assertEqual(observed['pid'],observed['sid'])
            self.assertNotIn(str(os.getpgrp()),proof.manifest.read()['groups'])

    def test_recovery_wrapper_spawns_broker_callers_in_recorded_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            b,proof=self.recorded_broker_proof(tmp)
            proof.profile.parent.mkdir(parents=True,exist_ok=True)
            proof.profile.write_text(json.dumps({'fabric':'plain','l1_address':proof.ports[0],'l3_address':proof.ports[1]}))
            cli=proof.home/'.local/bin/wefty';cli.parent.mkdir(parents=True)
            fixture=Path(__file__).with_name('testdata')/'rolling_upgrade_recorded_helper.py'
            cli.write_text('#!'+sys.executable+'\n'+fixture.read_text());cli.chmod(0o700)
            proof.lifecycle.__exit__(None,None,None)
            for command in (['status'],['doctor'],['upgrade','--rolling']):
                with self.subTest(command=command):
                    stdout=io.StringIO();stderr=io.StringIO()
                    env={'B1_FIXTURE_MANIFEST':str(proof.manifest.path),'B1_FIXTURE_BROKER':str(proof.broker),'PYTHONDONTWRITEBYTECODE':'1'}
                    original=b.safe_cli
                    def isolated(*args,**kwargs):
                        # Keep endpoint sanitation; supply only fixture inputs
                        # which clean_env intentionally does not inherit.
                        argv,clean=original(*args,**kwargs);clean.update(env);return argv,clean
                    with patch.dict(os.environ,env),patch.object(b,'safe_cli',side_effect=isolated),patch.object(b.Proof,'guard'),patch.object(b.Proof,'disk_guard'),patch.object(sys,'argv',['runner','--proof-command',str(proof.evidence),'--']+command),contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
                        code=b.main()
                    self.assertEqual(code,0,stderr.getvalue())
                    argv=json.loads(stdout.getvalue())['command']
                    self.assertEqual(argv[-len(command):],command)
                    self.assertEqual(argv[:8],['--profile',str(proof.profile),'--fabric','plain','--l1',proof.ports[0],'--l3',proof.ports[1]])

    def test_runner_own_group_cannot_be_registered_or_signalled(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=h.Round3().managed(tmp)
            with m.controller(),patch.object(s.os,'killpg') as signal_group:
                with self.assertRaisesRegex(RuntimeError,'unsafe spawned group'):m.group(os.getpgrp(),'unused')
                with self.assertRaisesRegex(RuntimeError,'unsafe group'):s.stop_group(os.getpgrp(),'unused')
            signal_group.assert_not_called()
            self.assertNotIn(str(os.getpgrp()),m.read()['groups'])

    def test_service_journals_cannot_adopt_the_readers_own_group(self):
        for operation in ('broker','teardown'):
            with self.subTest(operation=operation),tempfile.TemporaryDirectory() as tmp:
                m=h.Round3().managed(tmp)
                def invalid_child(args,**kwargs):
                    kwargs['journal'](os.getpgrp(),'invalid-self-child',True)
                    return 0,gui_output()
                with patch.object(s,'system_launchctl',side_effect=invalid_child):
                    if operation=='broker':
                        with self.assertRaises(RuntimeError):
                            s.launchctl(m,['print','gui/'+str(os.getuid())])
                    else:
                        with self.assertRaises(RuntimeError):s.teardown(m,stop=Mock())
                self.assertNotIn(os.getpgrp(),m.owned,'service journal adopted its own group')
                self.assertNotIn(os.getpgrp(),m.children)
                self.assertNotIn(str(os.getpgrp()),m.read()['groups'])

    def observed_gui_fixture(self):
        # Reconstructed from orchestrator-observed macOS 27.0.1 shapes and
        # counts. All labels, metadata and process IDs are synthetic/redacted.
        return Path(__file__).with_name('testdata').joinpath('rolling-upgrade-gui-macos27.txt').read_bytes()

    def accept_observed_gui(self,out,expected):
        try:labels=s.gui_domain_labels(out,'gui/502')
        except RuntimeError as error:self.fail('observed GUI inventory refused: '+str(error))
        self.assertEqual(labels,expected)

    def test_observed_login_domain_inventory(self):
        out=self.observed_gui_fixture()
        statuses=[line.split()[1] for line in out.decode().splitlines() if 'com.example.redacted.' in line]
        self.assertEqual(len(statuses),480)
        self.assertEqual([statuses.count(value) for value in ('-','(pe)','(jt)')],[236,198,1])
        self.accept_observed_gui(out,set())

    def test_observed_annotations_in_gui_domain(self):
        out=self.observed_gui_fixture().replace(b'type = login',b'type = gui')
        self.accept_observed_gui(out,set())
        # The documented signed-integer grammar includes an explicit plus.
        signed=out.replace(b'     2 \t',b'    +2 \t')
        self.assertNotEqual(out,signed)
        self.accept_observed_gui(signed,set())

    def test_observed_inventory_reports_proof_label(self):
        label=s.PREFIX+'a'*32+'.l1'
        out=self.observed_gui_fixture().replace(b'com.example.redacted.0240',label.encode())
        self.accept_observed_gui(out,{label})

    def test_observed_inventory_unknown_shapes_remain_incomplete(self):
        out=self.observed_gui_fixture()
        for status in (b'(PE)',b'(abcdefghi)',b'()',b'(p1)',b'pe',b'--',b'1.5'):
            with self.subTest(status=status),self.assertRaises(RuntimeError):
                s.gui_domain_labels(out.replace(b'(pe)',status,1),'gui/502')
        for invalid in (out.replace(b'service count = 480',b'service count = 479'),
                        out.replace(b'type = login',b'type = user')):
            with self.subTest(invalid=invalid[:100]),self.assertRaises(RuntimeError):
                s.gui_domain_labels(invalid,'gui/502')

    def assert_incomplete_without_deletion(self,out):
        with tempfile.TemporaryDirectory() as tmp:
            m=h.Round3().managed(tmp);root=Path(m.read()['root'])
            def fake(args):
                self.assertEqual(args,['print','gui/'+str(os.getuid())])
                return 0,out
            with patch.object(s,'delete_open_tree') as delete:
                try:s.teardown(m,invoke=fake)
                except RuntimeError:pass
                self.assertEqual(m.read()['teardown'],'incomplete','unreviewed grammar authorized absence')
                delete.assert_not_called();self.assertTrue(root.exists())

    def test_unknown_lowercase_annotations_keep_runtime(self):
        for status in (b'(xx)',b'(unknown)'):
            with self.subTest(status=status):
                out=gui_output(['com.example.redacted']).replace(b'0 - ',b'0 '+status+b' ')
                self.assert_incomplete_without_deletion(out)

    def test_inline_or_additional_services_blocks_keep_runtime(self):
        for block in (b'services = {}',b'services = { }',b'services = {\n}',b'services={}'):
            with self.subTest(block=block):
                out=gui_output().rstrip()[:-1]+block+b'\n}\n'
                self.assert_incomplete_without_deletion(out)

    def test_empty_caller_context_cannot_hide_omitted_gui_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=h.Round3().managed(tmp);root=Path(m.read()['root']);label=m.read()['prefix']+'l1'
            loaded={label};calls=[];stop=Mock()
            def fake(args):
                calls.append(args)
                if args==['list']:return 0,b'PID Status Label\n' # wrong bootstrap context
                if args==['print','gui/'+str(os.getuid())]:return 0,gui_output(sorted(loaded))
                if args[0]=='print':return (0,b'loaded') if args[1].endswith('/'+label) else (113,b'')
                if args[0]=='bootout':loaded.clear();return 0,b''
                raise AssertionError('unexpected fake service action: '+str(args))
            try:s.teardown(m,invoke=fake,stop=stop)
            except RuntimeError:pass
            self.assertTrue(root.exists(),'empty caller list authorized GUI runtime deletion')
            self.assertEqual(m.read()['teardown'],'incomplete')
            self.assertEqual(loaded,{label});stop.assert_not_called()
            self.assertNotIn(['list'],calls,'reconciliation used caller bootstrap context')
            self.assertIn(['print','gui/'+str(os.getuid())],calls)

    def archive(self,files):
        out=io.BytesIO()
        with tarfile.open(fileobj=out,mode='w') as tar:
            for name,data in sorted(files.items()):
                entry=tarfile.TarInfo(name);entry.size=len(data)
                tar.addfile(entry,io.BytesIO(data))
        return out.getvalue()

    def assert_changed_archive_refused(self,b,reviewed,changed):
        import subprocess
        approved=b.compilation_fingerprint(self.archive(reviewed))
        payload=self.archive(changed);sha='1'*40
        def git(args,**kwargs):
            if args==['git','rev-parse','--show-toplevel']:out=str(REPO).encode()
            elif args[:4]==['git','rev-parse','--verify','--end-of-options']:out=sha.encode()
            elif args[:3]==['git','ls-tree','-rlz']:out=b'100644 blob '+b'2'*40+b' 100\truntime.go\0'
            elif args==['git','archive','--format=tar',sha]:out=payload
            else:raise AssertionError('unexpected Git operation: '+str(args))
            return subprocess.CompletedProcess(args,0,out.decode() if kwargs.get('text') else out,b'')
        args=types.SimpleNamespace(repo=str(REPO),refs=['changed']*3)
        with patch.object(b,'source_policy',return_value=dict(POLICY,compilation_fingerprint=approved)),patch.object(b.subprocess,'run',side_effect=git):
            _,refs=b.resolved_refs(args)
        self.assertEqual([accepted for _,_,accepted in refs],[False]*3,'changed embedded compilation input passed compatibility')

    def test_embedded_test_named_asset_addition_or_change_refuses_compatibility(self):
        b=h.runner()
        base={'skills/skills.go':b'package skills\nimport "embed"\n//go:embed wefty\nvar Wefty embed.FS\n',
              'skills/wefty/SKILL.md':b'reviewed skill'}
        for old in (None,b'old embedded bytes'):
            with self.subTest(old=old):
                reviewed=dict(base);changed=dict(base)
                if old is not None:reviewed['skills/wefty/example_test.go']=old
                changed['skills/wefty/example_test.go']=b'changed embedded bytes'
                self.assert_changed_archive_refused(b,reviewed,changed)

    def test_embed_patterns_override_test_and_directory_exclusions(self):
        b=h.runner()
        for pattern,name in [('assets/*','pkg/assets/example_test.go'),
                             ('resource_test.go','pkg/resource_test.go'),
                             ('all:assets','pkg/assets/_hidden_test.go'),
                             ('docs','docs/example_test.go')]:
            with self.subTest(pattern=pattern):
                source='runtime.go' if pattern=='docs' else 'pkg/runtime.go'
                reviewed={source:('package fixture\nimport "embed"\n//go:embed '+pattern+'\nvar Assets embed.FS\n').encode(),name:b'old asset'}
                self.assert_changed_archive_refused(b,reviewed,dict(reviewed,**{name:b'changed asset'}))

    def test_unembedded_go_tests_remain_excluded(self):
        b=h.runner();reviewed={'runtime.go':b'package fixture\n'}
        changed=dict(reviewed,**{'runtime_test.go':b'package fixture\n'})
        self.assertEqual(b.compilation_fingerprint(self.archive(reviewed)),b.compilation_fingerprint(self.archive(changed)))

    def test_amended_and_merged_sources_with_identical_inputs_pass(self):
        b=h.runner()
        with source_history() as (repo,reviewed,amended,merged,changed):
            policy=dict(POLICY,reviewed_commit=reviewed,compilation_fingerprint=FINGERPRINT)
            args=types.SimpleNamespace(repo=str(repo),refs=[reviewed,amended,merged])
            with patch.object(b,'source_policy',return_value=policy):resolved,refs=b.resolved_refs(args)
            self.assertEqual(resolved,repo)
            self.assertTrue(all(accepted for _,_,accepted in refs),'compatible amended/merged SHA rejected: '+str(refs))
            self.assertEqual(len({sha for _,sha,_ in refs}),3)

    def test_packet_names_harness_sources_and_reviewed_fingerprint(self):
        import subprocess
        b=h.runner();out=io.StringIO()
        with source_history() as (repo,reviewed,amended,merged,changed):
            policy=dict(POLICY,reviewed_commit=reviewed,compilation_fingerprint=FINGERPRINT)
            args=types.SimpleNamespace(repo=str(repo),refs=[reviewed,amended,merged],evidence_dir=None)
            with patch.object(b,'source_policy',return_value=policy),contextlib.redirect_stdout(out):b.plan(args)
            packet=out.getvalue()
            head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
            self.assertIn('Harness commit: '+head,packet)
            self.assertIn('Reviewed compilation fingerprint: '+FINGERPRINT,packet)
            self.assertEqual(packet.count('compilation policy PASS'),3)
            command=next(line for line in packet.splitlines() if line.startswith('Execute only after approval:'))
            self.assertIn('--refs '+reviewed+' '+amended+' '+merged,command)
            self.assertNotIn('--refs HEAD',command)
            self.assertIn('--harness-commit '+head,command)

    def test_failed_wrong_domain_or_malformed_gui_inventory_keeps_files(self):
        valid=gui_output()
        cases=[(5,b'GUI unavailable'),(0,gui_output(domain='user/'+str(os.getuid()))),
               (0,valid[:-2]),(0,valid.replace(b'service count = 0',b'service count = 1')),
               (0,valid.replace(b'type = gui',b'type = user')),
               (0,b'PID Status Label\n'),
               (0,gui_output(['duplicate','duplicate'])),
               (0,valid.replace(b'services = {',b'endpoints = {'))]
        for response in cases:
            with self.subTest(response=response),tempfile.TemporaryDirectory() as tmp:
                m=h.Round3().managed(tmp);calls=[]
                def fake(args):
                    calls.append(args)
                    self.assertEqual(args,['print','gui/'+str(os.getuid())])
                    return response
                with self.assertRaises(RuntimeError):s.teardown(m,invoke=fake)
                self.assertTrue(Path(m.read()['root']).exists())
                self.assertEqual(m.read()['teardown'],'incomplete');self.assertTrue(calls)
                self.assertTrue(all(args==['print','gui/'+str(os.getuid())] for args in calls))

    def test_complete_gui_inventory_ignores_shared_endpoints_and_allows_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=h.Round3().managed(tmp);domain='gui/'+str(os.getuid());calls=[]
            # A Mach name can be shared from a different user domain. It is
            # deliberately outside this GUI domain's discrete services table.
            out=gui_output().rstrip()[:-1]+('\tendpoints = {\n\t\t'+m.read()['prefix']+'l1 = {\n\t\t}\n\t}\n}\n').encode()
            def fake(args):calls.append(args);self.assertEqual(args,['print',domain]);return 0,out
            s.teardown(m,invoke=fake)
            self.assertFalse(Path(m.read()['root']).exists());self.assertEqual(m.read()['teardown'],'absent')
            self.assertEqual(len(calls),2)

    def test_broker_refuses_caller_context_and_foreign_domains(self):
        with tempfile.TemporaryDirectory() as tmp:
            m=h.Round3().managed(tmp);fake=Mock()
            for args in (['list'],['print','user/'+str(os.getuid())],['print','gui/'+str(os.getuid()+1)]):
                with self.assertRaises(RuntimeError):s.launchctl(m,args,invoke=fake)
            fake.assert_not_called()
            domain='gui/'+str(os.getuid());fake.return_value=(0,gui_output([m.read()['prefix']+'l1']))
            s.launchctl(m,['print',domain],invoke=fake)
            fake.assert_called_once_with(['print',domain]);self.assertEqual(m.read()['labels'],{})

    def test_changed_inputs_refuse_even_when_provenance_names_compatible_head(self):
        b=h.runner()
        with source_history() as (repo,reviewed,amended,merged,changed):
            policy=dict(POLICY,reviewed_commit=merged,compilation_fingerprint=FINGERPRINT)
            args=types.SimpleNamespace(repo=str(repo),refs=[merged,changed,merged])
            with patch.object(b,'source_policy',return_value=policy),patch.object(b,'compilation_fingerprint',wraps=b.compilation_fingerprint) as fingerprint:
                _,refs=b.resolved_refs(args)
            self.assertEqual([accepted for _,_,accepted in refs],[True,False,True],'changed compilation inputs accepted')
            self.assertEqual(fingerprint.call_count,2,'requested unique source commits were not each fingerprinted')

    def test_default_source_is_current_checkout_not_provenance(self):
        b=h.runner()
        with source_history() as (repo,reviewed,amended,merged,changed):
            policy=dict(POLICY,reviewed_commit=reviewed,compilation_fingerprint=FINGERPRINT)
            args=types.SimpleNamespace(repo=str(repo),refs=None)
            with patch.object(b,'source_policy',return_value=policy):_,refs=b.resolved_refs(args)
            self.assertEqual([(sha,accepted) for _,sha,accepted in refs],[(merged,True)]*3)

    def test_dry_run_changed_inputs_returns_refusal_status(self):
        b=h.runner();out=io.StringIO()
        with source_history() as (repo,reviewed,amended,merged,changed):
            policy=dict(POLICY,reviewed_commit=changed,compilation_fingerprint=FINGERPRINT)
            with patch.object(sys,'argv',['runner','--dry-run','--repo',str(repo),'--refs',changed,changed,changed]),patch.object(b,'source_policy',return_value=policy),contextlib.redirect_stdout(out):
                self.assertEqual(b.main(),1)
            self.assertEqual(out.getvalue().count('compilation policy REFUSE'),3)

    def test_packet_checks_actual_harness_bytes_even_if_git_status_is_clean(self):
        import subprocess
        b=h.runner();out=io.StringIO();real_run=subprocess.run;read=Path.read_bytes
        resources=['scripts/measure-rolling-upgrade.py','scripts/rolling_upgrade_safety.py','scripts/testdata/rolling-upgrade-plan.go.tmpl','scripts/testdata/rolling-upgrade-source-policy.json']
        committed={REPO/name:subprocess.check_output(['git','show','HEAD:'+name],cwd=REPO) for name in resources}
        def git(args,**kwargs):
            if args[:3]==['git','diff','--quiet']:return subprocess.CompletedProcess(args,0,b'',b'')
            return real_run(args,**kwargs)
        def changed(path):
            data=committed[path] if path in committed else read(path)
            return data+b'\n# unreviewed execution bytes\n' if path==REPO/'scripts/rolling_upgrade_safety.py' else data
        args=types.SimpleNamespace(repo=str(REPO),refs=['HEAD']*3,evidence_dir=None)
        with patch.object(b,'source_policy',return_value=dict(POLICY)),patch.object(b.subprocess,'run',side_effect=git),patch.object(Path,'read_bytes',changed),contextlib.redirect_stdout(out):b.plan(args)
        self.assertIn('tracked execution resources: MODIFIED',out.getvalue(),'metadata cleanliness was mistaken for reviewed executing bytes')

if __name__=='__main__':unittest.main()
