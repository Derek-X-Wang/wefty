# Appended only to the temporary broker's copied safety module by the test.
# NEVER invokes launchctl. Exercise the broker's real child-journal callback.
def system_launchctl(args,timeout=10,journal=None,env=None):
    require(args[0]=='print' and args[1].startswith('gui/'+str(os.getuid())+'/dev.wefty-b1-proof.'),'unexpected fake service call')
    child=subprocess.Popen([sys.executable,'-c','import sys; sys.stdin.buffer.read()'],stdin=subprocess.PIPE,start_new_session=True)
    birth=process_birth(child.pid)
    try:
        if journal:journal(child.pid,birth,True)
        child.communicate(timeout=timeout)
        if journal:journal(child.pid,birth,False)
        return 113,b''
    finally:
        if child.poll() is None:
            child.communicate(timeout=timeout)
