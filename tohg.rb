require 'fromcvs'

require 'python'
require 'python/mercurial/ui'
require 'python/mercurial/localrepo'


class HGDestRepo
  def initialize(hgroot, status=lambda{|s|})
    @status = status

    ui = Py.mercurial.ui.ui(Py::KW, :interactive => false)
    @hgrepo = Py.mercurial.localrepo.localrepository(ui, hgroot)

    flush_tag_cache
    @tags = @hgrepo.tags
    unless @tags.include? 'HEAD'
      tag('HEAD', Py.mercurial.node.nullid)
    end
  end

  def last_date
    Time.at(@hgrepo.changelog.read(@hgrepo.changelog.tip)[2][0])
  end

  def filelist
  end

  def start
    @wlock = @hgrepo.wlock
    @transaction = @hgrepo.transaction
    @commits = 0
  end

  def flush
    return if @commits < 10
    @hgrepo.dirstate.setparents(Py::mercurial::node::nullid)  # prevent updating the dirstate
    @transaction.close
    @transaction = @hgrepo.transaction
    @commits = 0
  end

  def has_branch?(branch)
    @tags.include? branch
  end

  def create_branch(branch, parent, vendor_p)
    if vendor_p
      node = Py.mercurial.node.nullid
    else
      parent ||= 'HEAD'
      node = @tags[parent]
      status "creating branch #{branch} from #{parent}, cset #{node.unpack('H12')}"
    end
    tag(branch, node)
  end

  def select_branch(branch)
    @curbranch = branch || 'HEAD'
  end

  def remove(file)
    begin
      File.unlink(@hgrepo.wjoin(file))
    rescue Errno::ENOENT
      # well, it is gone already
    end
  end

  def update(file, data, mode, uid, gid)
    @hgrepo.wwrite(file, data)
    mode |= 0666
    mode &= ~File.umask
    File.chmod(mode, @hgrepo.wjoin(file))
  end

  def commit(author, date, msg, files)
    status "committing set by #{author} at #{date} to #@curbranch"
    _commit(author, date, msg, files)
  end

  def merge(branch, author, date, msg, files)
    status "merging cset #{branch.unpack('H12')[0]} by #{author} at #{date} to #@curbranch"
    _commit(author, date, msg, files, branch)
  end

  def finish
    @transaction.close
    flush_tag_cache
    @wlock.release
  end

  private
  def _commit(author, date, msg, files, p2=nil)
    p1 = @tags[@curbranch]
    @hgrepo.rawcommit(files, msg, author, "#{date.to_i} 0", p1, p2, @wlock)
    @commits += 1
    tag(@curbranch, @hgrepo.changelog.tip)
  end

  private
  def tag(branch, node)
    @tags[branch] = node
    @hgrepo.tag(branch, node, nil, true, nil, nil)
    node
  end

  private
  def flush_tag_cache
    @tags = @hgrepo.tags
    tf = @hgrepo.opener('localtags', 'w')
    tf.truncate(0)
    @tags.each do |branch, node|
      next if branch == 'tip'
      tf.write("#{node.unpack('H*')} #{branch}\n")
    end
    tf.close
    @hgrepo.reload    # just to be sure
    @tags = @hgrepo.tags
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

  cvsrepo = Repo.new(cvsdir, modul, status)
  hgrepo = HGDestRepo.new(hgdir, status)
  cvsrepo.convert(hgrepo)
end
