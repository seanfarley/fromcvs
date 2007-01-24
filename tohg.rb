require 'fromcvs'

require 'python'
require 'python/mercurial/ui'
require 'python/mercurial/localrepo'

module FromCVS

class HGDestRepo
  attr_reader :revs_with_cset
  attr_reader :revs_per_file

  def initialize(hgroot, status=lambda{|s|})
    @revs_per_file = false
    @revs_with_cset = true
    @status = status

    ui = Py.mercurial.ui.ui(Py::KW, :interactive => false)
    @hgrepo = Py.mercurial.localrepo.localrepository(ui, hgroot)
  end

  def last_date
    Time.at(@hgrepo.changelog.read(@hgrepo.changelog.tip)[2][0])
  end

  def filelist(tag)
    if tag == :complete
      # XXX to be implemented
    else
      node = @hgrepo.branchtags[tag || 'HEAD']
      @hgrepo.manifest.read(@hgrepo.changelog.read(node)[0]).keys
    end
  end

  def start
    @wlock = @hgrepo.wlock
    @transaction = @hgrepo.transaction
    @commits = 0
    @files = []
  end

  def flush(force=false)
    return if @commits < 10 and not force
    @hgrepo.dirstate.setparents(Py::mercurial::node::nullid)  # prevent updating the dirstate
    @transaction.close
    @transaction = @hgrepo.transaction
    @commits = 0
  end

  def has_branch?(branch)
    @hgrepo.branchtags.include?(branch || 'HEAD')
  end

  def branch_id(branch)
    @hgrepo.branchtags[branch || 'HEAD']
  end

  def create_branch(branch, parent, vendor_p, date)
    return if @hgrepo.branchtags.include? branch

    if vendor_p
      node = Py.mercurial.node.nullid
      text = "creating vendor branch #{branch}"
    else
      parent ||= 'HEAD'
      node = @hgrepo.branchtags[parent]
      status "creating branch #{branch} from #{parent}, cset #{node.unpack('H12')}"
      text = "creating branch #{branch} from #{parent}"
    end

    newnode = _commit("repo convert", date, text, [], nil, node, branch)
    @hgrepo.branchtags[branch] = newnode    # mercurial does not keep track itself
    newnode
  end

  def select_branch(branch)
    @curbranch = branch || 'HEAD'
  end

  def remove(file, rev)
    begin
      File.unlink(@hgrepo.wjoin(file))
    rescue Errno::ENOENT
      # well, it is gone already
    end
    @files << file
  end

  def update(file, data, mode, uid, gid, rev)
    if mode & 0111 != 0
      mode = "x"
    else
      mode = ""
    end
    @hgrepo.wwrite(file, data, mode)
    @files << file
  end

  def commit(author, date, msg, revs)
    status "committing set by #{author} at #{date} to #@curbranch"
    node = _commit(author, date, msg, @files)
    @files = []
    node
  end

  def merge(branch, author, date, msg, revs)
    status "merging cset #{branch.unpack('H12')[0]} by #{author} at #{date} to #@curbranch"
    node = _commit(author, date, msg, @files, branch)
    @files = []
    node
  end

  def finish
    @transaction.close
    @wlock.release
  end

  private
  def _commit(author, date, msg, files, p2=nil, p1=nil, branch=nil)
    # per default commit to @curbranch
    p1 ||= @hgrepo.branchtags[@curbranch]
    branch ||= @curbranch

    # if can happen that HEAD has not been created yet
    if not p1
      if @curbranch != "HEAD"
        raise StandardError, "branch #{@curbranch} not yet created"
      end
      p1 = Py.mercurial.node.nullid
    end

    @commits += 1
    node = @hgrepo.rawcommit(Py::KW,
                      :files => files,
                      :text => msg,
                      :user => author,
                      :date => "#{date.to_i} 0",
                      :p1 => p1,
                      :p2 => p2,
                      :wlock => @wlock,
                      # we have to convert to a native
                      # Python dict here, because mercurial
                      # does extra.copy().  Actually we
                      # could also alias #copy with #dup for
                      # this very object.
                      :extra => Py::dict({"branch" => branch}))
    @hgrepo.branchtags[branch] = node   # not kept up-to-date by mercurial :/
    node
  end

  def status(str)
    @status.call(str) if @status
  end
end


if $0 == __FILE__
  status = lambda do |str|
    puts str
  end

  if ARGV.length != 3
    puts "call: tohg <cvsroot> <module> <hgdir>"
    exit 1
  end

  cvsdir, modul, hgdir = ARGV

  hgrepo = HGDestRepo.new(hgdir, status)
  cvsrepo = Repo.new(cvsdir, hgrepo)
  cvsrepo.status = status
  cvsrepo.scan(modul)
  cvsrepo.commit_sets
end

end   # module FromCVS
