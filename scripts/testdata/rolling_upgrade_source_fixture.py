"""Disposable real Git history; never reads or changes another checkout."""
import contextlib
import os
from pathlib import Path
import subprocess
import tempfile

# Fixed independently reviewed bytes and their compilation-input fingerprint.
SOURCE=b'package fixture\nconst Value = 1\n'
FINGERPRINT='be930ad5139c4e120675dc2d3cd7abca515b53a3eaf68fb485adf98421befcde'

@contextlib.contextmanager
def source_history():
    with tempfile.TemporaryDirectory(prefix='wefty-b1-source-test-') as tmp:
        repo=Path(tmp).resolve()
        env={key:os.environ[key] for key in ('PATH','TMPDIR','LANG','LC_ALL') if key in os.environ}
        env.update(HOME=str(repo),GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1',
                   GIT_AUTHOR_NAME='B1 fixture',GIT_AUTHOR_EMAIL='fixture@example.invalid',
                   GIT_COMMITTER_NAME='B1 fixture',GIT_COMMITTER_EMAIL='fixture@example.invalid')
        def git(*args):
            result=subprocess.run(['git','-c','core.hooksPath=/dev/null','-c','commit.gpgsign=false']+list(args),
                cwd=repo,env=env,capture_output=True,text=True,timeout=5)
            if result.returncode:raise RuntimeError('disposable Git fixture failed: '+result.stderr)
            return result.stdout.strip()
        git('init','-q','-b','proof')
        (repo/'runtime.go').write_bytes(SOURCE)
        git('add','runtime.go');git('commit','-qm','reviewed source');reviewed=git('rev-parse','HEAD')
        git('branch','reviewed',reviewed)
        git('commit','--amend','--allow-empty','-qm','amended identity');amended=git('rev-parse','HEAD')
        git('merge','--allow-unrelated-histories','--no-ff','-qm','merged identity','reviewed');merged=git('rev-parse','HEAD')
        (repo/'runtime.go').write_bytes(SOURCE.replace(b'1',b'2'))
        git('add','runtime.go');git('commit','-qm','changed runtime');changed=git('rev-parse','HEAD')
        git('checkout','-q','--detach',merged)
        yield repo,reviewed,amended,merged,changed
