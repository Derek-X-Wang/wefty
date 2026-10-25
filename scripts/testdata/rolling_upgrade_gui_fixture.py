"""Synthetic GUI-domain output only. No native launchd evidence."""
import os


def gui_output(labels=(),domain=None):
    domain=domain or 'gui/'+str(os.getuid())
    return (domain+' = {\n\ttype = gui\n\tservice count = '+str(len(labels))+'\n\tservices = {\n'+''.join('\t\t0 - '+label+'\n' for label in labels)+'\t}\n}\n').encode()


def empty_services(args):
    return (0,gui_output()) if args==['print','gui/'+str(os.getuid())] else (113,b'absent')
