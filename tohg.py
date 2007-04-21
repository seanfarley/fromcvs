import sys
import time
import mercurial
from mercurial import ui, localrepo, node

class HgDestRepo:
    def __init__(self, ins, outs, hgroot):
        self.ins = ins
        self.outs = outs
        self.ui = ui.ui(interactive = False)
        self.hgrepo = localrepo.localrepository(self.ui, hgroot)
        self.start()
        self.last_date()
        self.branchlist()
    
    def start(self):
        self.wlock = self.hgrepo.wlock()
        self.transaction = self.hgrepo.transaction()

    def last_date(self):
        date = self.hgrepo.changelog.read(self.hgrepo.changelog.tip())[2][0]
        self.outs.write("%d\n" % date)
        self.outs.flush()

    def branchlist(self):
        for br, n in self.hgrepo.branchtags().items():
            self.outs.write("%s %s\n" % (br, node.hex(n)))
        self.outs.write("\n")
        self.outs.flush()

    def cmd_filelist(self, n):
        # XXX tag == complete
        n = node.bin(n)
        files = self.hgrepo.manifest.read(self.hgrepo.changelog.read(n)[0]).keys()
        for f in files:
            self.outs.write("%s\0" % f)
        self.outs.write("\0\n")
        self.outs.flush()

    def cmd_flush(self):
        # prevent updating the dirstate
        self.hgrepo.dirstate.setparents(node.nullid)
        self.transaction.close()
        self.transaction = self.hgrepo.transaction()

    def cmd_commit(self):
        user = self.ins.readline().strip()
        date = self.ins.readline().strip()
        p1 = self.ins.readline().strip()
        if p1:
            p1 = node.bin(p1)
        p2 = self.ins.readline().strip()
        if p2:
            p2 = node.bin(p2)
        branch = self.ins.readline().strip()
        filestr = ''
        files = []
        while 1:
            filestr += self.ins.readline()
            l = filestr.split("\0")
            files += l[0:-1]
            filestr = l[-1]
            if len(l) > 1 and l[-2] == '':
                break
        files.pop()     # shift out terminator
        textlen = int(self.ins.readline().strip())
        text = self.ins.read(textlen)
        if not self.ins.readline():   # eat newline after text
            raise RuntimeError, 'bad input stream: invalid commit'

        n = self.hgrepo.commit(files = files,
                               text = text,
                               user = user,
                               date = "%s 0" % date,
                               p1 = p1,
                               p2 = p2,
                               wlock = self.wlock,
                               extra = {'branch': branch})

        if not n:
            raise RuntimeError, "commit by %s at %s (%s) did not succeed" % \
                    (user, time.asctime(time.gmtime(int(date))), ", ".join(files))

        self.outs.write("%s\n" % node.hex(n))
        self.outs.flush()

    def cmd_finish(self):
        self.transaction.close()
        self.wlock.release()

    def dispatch(self):
        while 1:
            l = self.ins.readline()
            if not l:
                break
            l = l.strip().split()
            func = getattr(self, 'cmd_'+l[0])
            func(*l[1:])

if __name__ == '__main__':
    destrepo = HgDestRepo(sys.stdin, sys.stdout, sys.argv[1])
    destrepo.dispatch()
