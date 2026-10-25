#!/usr/bin/env python3
"""Targeted mutation-red checks. Only fake service/process tests are invoked."""
import sys
sys.dont_write_bytecode=True
import os
from pathlib import Path
import subprocess
import tempfile

scripts=Path(__file__).resolve().parent
runner=(scripts/'measure-rolling-upgrade.py').read_text()
safety=(scripts/'rolling_upgrade_safety.py').read_text()

def replace(source,before,after):
    assert source.count(before)==1,before
    return source.replace(before,after)

cases=[
 ('source-fingerprint-rejection','runner',"compilation_fingerprint(archive.stdout)==policy['compilation_fingerprint']",'True','round4','test_changed_inputs_refuse_even_when_provenance_names_compatible_head'),
 ('complete-path-validation','safety',"require(path.is_absolute() and '..' not in path.parts and (path==root or root in path.parents),\n                    'manifest deletion target outside proof root')",'pass', 'round2','test_manifest_daily_path_sentinel_is_never_a_deletion_target'),
 ('escalation-identity','safety','        check();os.killpg(pgid,signal.SIGKILL)','        os.killpg(pgid,signal.SIGKILL)', 'round2','test_mixed_group_is_not_signalled_and_escalation_rechecks_birth'),
 ('atomic-start-closing','safety',"require(os.pread(self.controller_fd,1,0)==b'R','proof closing; controller must stop')",'pass', 'round2','test_spawn_registration_survives_teardown_during_popen'),
 ('unknown-session-survivor','safety','            if sid in self.owned:uncertain.append(pid);ambiguous.add(row[-1])','            pass', 'round2','test_orphaned_session_member_blocks_absence_without_adoption'),
 ('manual-controller-lease','safety','            deadline=time.monotonic()+timeout\n            while True:\n                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);break','            deadline=time.monotonic()+timeout\n            while True:\n                try:break', 'round2','test_active_controller_excludes_manual_recovery_lease'),
 ('manual-orphan-exclusion','runner','                m.require_idle(d)','                pass', 'round2','test_orphaned_command_group_blocks_manual_recovery_before_spawn'),
 ('manual-exit-status','runner','return result.returncode if result.returncode>=0 else 128-result.returncode','return 0', 'round2','test_manual_cancel_failure_and_real_refusal_response_are_visible'),
 ('pending-crash-evidence','runner','        for identity in crashed:','        for identity in ():', 'round1','test_r4_crashed_retiree_is_retained_not_deleted'),
 ('no-op-promotion','runner','require(not result.get("already_at") and before["primary_generation_id"]!=after["primary_generation_id"]\n                and bool(after.get("operation_id")) and before.get("operation_id")!=after["operation_id"],\n                "upgrade was a no-op or primary/promotion operation did not change")','return', 'round1','test_r5_noop_upgrade_cannot_count_as_cutover'),
 ('exact-attempt-settlement','runner','require(last["job_id"]==original["job"] and len(last.get("attempts",[]))==1\n                    and last["attempts"][0]["attempt_id"]==original["attempt"],"replacement Attempt during settlement")','pass', 'round1','test_r9_successor_and_timeout_are_not_settlement'),
 ('repeat-termination','runner','for sig in previous:signal.signal(sig,signal.SIG_IGN)','for sig in previous:signal.signal(sig,signal.SIG_DFL)', 'round2','test_second_ctrl_c_during_actual_cleanup_is_ignored'),
]
with tempfile.TemporaryDirectory() as tmp:
    for name,target,before,after,lane,test in cases:
        source=safety if target=='safety' else runner
        # The repeated-termination policy is used in both cleanup entry paths.
        mutated=source.replace(before,after) if name=='repeat-termination' else replace(source,before,after)
        file=Path(tmp)/(name+'.py');file.write_text(mutated)
        env=dict(os.environ);env['B1_SAFETY' if target=='safety' else 'B1_RUNNER']=str(file)
        script,cls={'round1':('test-rolling-upgrade.py','SafetyReview'),'round2':('test-rolling-upgrade-round2.py','Round2'),'round4':('test-rolling-upgrade-round4.py','Round4')}[lane]
        result=subprocess.run([sys.executable,str(scripts/script),cls+'.'+test],env=env,capture_output=True,text=True,timeout=15)
        output=result.stdout+result.stderr
        if result.returncode==0 or 'FAIL:' not in output or 'ERROR:' in output:
            raise SystemExit('Mutation did not produce behavioral red: '+name+'\n'+output)
        print(name+': behavioral RED (assertion failure, no missing-seam error)')
print(str(len(cases))+' targeted mutations rejected')
