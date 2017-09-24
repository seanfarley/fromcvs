import os
import sys
import time
from mercurial import context, localrepo, node, ui


class HgDestRepo:
    def __init__(self, ins, outs, hgroot):
        self.ins = ins
        self.outs = outs
        self.ui = ui.ui()
        self.hgrepo = localrepo.localrepository(self.ui, hgroot)
        self.start()
        self.last_date()
        self.branchlist()

    def start(self):
        self.wlock = self.hgrepo.wlock()
        self.transaction = self.hgrepo.transaction('fromcvs')

    def last_date(self):
        date = self.hgrepo.changelog.read(self.hgrepo.changelog.tip())[2][0]
        self.outs.write("%d\n" % date)
        self.outs.flush()

    def branchlist(self):
        for br, nodes in self.hgrepo.branchmap().iteritems():
            for n in nodes:
                self.outs.write("%s %s\n" % (br, node.hex(n)))
        self.outs.write("\n")
        self.outs.flush()

    def cmd_filelist(self, n):
        n = node.bin(n)
        files = self.hgrepo[n].manifest().keys()
        for f in files:
            self.outs.write("%s\0" % f)
        self.outs.write("\0\n")
        self.outs.flush()

    def cmd_flush(self):
        self.transaction.close()
        del self.transaction
        self.transaction = self.hgrepo.transaction('fromcvs')

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
            raise RuntimeError('bad input stream: invalid commit')

        similarity = 0 / 100.0         # arbitrary choice
        renames = {}
        def getfilectx(repo, memctx, f):
            fpath = os.path.join(repo.root, f)
            data = None
            try:
                with open(fpath) as fp:
                    data = fp.read()
                return context.memfilectx(repo, f, data, bool(os.path.islink(fpath)),
                                          bool(os.access(fpath, os.X_OK)),
                                          renames.get(f))
            except IOError:
                pass
            return None

        extra = {'branch': branch}
        memctx = context.memctx(self.hgrepo, (p1, p2), text, set(files),
                                getfilectx, user, "%s 0" % date, extra)

        with self.hgrepo.transaction('fromcvs-commit'):
            n = self.hgrepo.commitctx(memctx)

        if not n:
            raise RuntimeError("commit by %s at %s (%s) did not succeed" %
                               (user, time.asctime(time.gmtime(int(date))),
                                ", ".join(files)))

        self.outs.write("%s\n" % node.hex(n))
        self.outs.flush()

    def cmd_finish(self):
        self.transaction.close()
        del self.transaction
        self.wlock.release()

    def dispatch(self):
        while 1:
            l = self.ins.readline()
            if not l:
                break
            l = l.strip().split()
            func = getattr(self, 'cmd_' + l[0])
            func(*l[1:])

if __name__ == '__main__':
    destrepo = HgDestRepo(sys.stdin, sys.stdout, sys.argv[1])
    destrepo.dispatch()
