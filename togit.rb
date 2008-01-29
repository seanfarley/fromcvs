require 'fromcvs'

require 'enumerator'

class RCSFile::Rev
  attr_accessor :git_aux
end

module FromCVS

# We are outputting a git-fast-import stream

class GitDestRepo
  attr_reader :revs_with_cset
  attr_reader :revs_per_file

  def initialize(gitroot, status=lambda{|s|})
    @revs_per_file = true
    @revs_with_cset = false
    @status = status

    @gitroot = gitroot
    if File.directory?(File.join(@gitroot, '.git'))
      @gitroot = File.join(@gitroot, '.git')
    end
    if not File.directory?(@gitroot)
      raise Errono::ENOENT, "dest dir `#@gitroot' is no git repo"
    end
    ENV['GIT_DIR'] = @gitroot

    @deleted = []
    @modified = []
    @branchcache = {}
    @files = Hash.new{|h,k| h[k] = {}}

    @mark = 0

    @gfi = IO.popen('-', 'w')
    if not @gfi   # child
      exec('git-fast-import')
      $stderr.puts "could not spawn git-fast-import"
      exit 1
    end
  end

  def last_date
    latestref = _command(*%w{git-for-each-ref --count=1 --sort=-committerdate 
                                --format=%(refname) refs/heads})
    log = _command('git-cat-file', '-p', latestref.strip)
    log.split("\n").each do |line|
      break if line.empty?
      line = line.split
      return Time.at(line[-2].to_i) if line[0] == "committer"
    end

    if log.empty?
      return Time.at(0)
    end

    raise RuntimeError, "Invalid output from git"
  end

  def filelist(tag)
    if tag == :complete
      # XXX to be implemented
    else
      tag ||= 'master'

      return @files[tag].keys if @files.has_key? tag

      files = _command(*(%w{git-ls-tree --name-only --full-name -r -z} +
                         ["refs/heads/#{tag}"])).split("\0")
      files.collect! do |f|
        _unquote(f)
      end

      @files[tag] = Hash[*files.map{|f| [f, true]}.flatten]
      files
    end
  end

  def start
    _command(*%w{git-for-each-ref}).split("\n").each do |line|
      sha, type, branch = line.split
      next if type != 'commit'
      branch[/^.*\//] = ""
      @branchcache[branch] = sha
    end
    @pickupbranches = @branchcache.dup
  end

  def flush
  end

  def has_branch?(branch)
    @branchcache.has_key?(branch || 'master')
  end

  def branch_id(branch)
    @branchcache[branch || 'master']
  end

  # This requires that no commits happen to the parent before
  # we don't commit to the new branch
  def create_branch(branch, parent, vendor_p, date)
    parent ||= 'master'

    if @branchcache.has_key?(branch)
      raise RuntimeError, "creating existant branch"
    end

    @gfi.puts "reset refs/heads/#{branch}"

    # branchcache[parent] can be nil, because we could
    # happen to branch before the first commit.
    # In this case, we're a new branch like a vendor branch.
    if not vendor_p and @branchcache[parent]
      @gfi.puts "from #{@branchcache[parent]}"
    end
    @gfi.puts
    @branchcache[branch] = @branchcache[parent]
  end

  def select_branch(branch)
    @curbranch = _quote(branch || 'master')
  end

  def remove(file, rev)
    rev.git_aux = [_quote(file), nil, nil]
  end

  def update(file, data, mode, uid, gid, rev)
    @mark += 1
    @gfi.print <<-END
blob
mark :#@mark
data #{data.size}
#{data}
    END
    # Fix up mode for git
    if mode & 0111 != 0
      mode |= 0111
    end
    mode &= ~022
    mode |= 0644
    rev.git_aux = [_quote(file), mode, @mark]
  end

  def commit(author, date, msg, revs)
    _commit(author, date, msg, revs)
  end

  def merge(branch, author, date, msg, revs)
    _commit(author, date, msg, revs, branch)
  end

  def finish
    @gfi.close_write
    raise RuntimeError, "git-fast-import did not succeed" if $?.exitstatus != 0
  end

  private

  def _commit(author, date, msg, revs, branch=nil)
    @mark += 1
    if author !~ /<.+>/
      # fake email address
      author = "#{author} <#{author}>"
    end
    @gfi.print <<-END
commit refs/heads/#@curbranch
mark :#@mark
committer #{author} #{date.to_i} +0000
data #{msg.size}
#{msg}
    END
    if @pickupbranches.has_key? @curbranch
      @pickupbranches.delete(@curbranch)
      # fix incremental runs, force gfi to pick up
      @gfi.puts "from refs/heads/#@curbranch^0"
    end
    if branch
      @gfi.puts "merge :#{branch}"
    end
    revs.each do |rev|
      f, mode, mark = rev.git_aux
      if mode
        @gfi.puts "M #{mode.to_s(8)} :#{mark} #{f}"
        @files[@curbranch][f] = true
      else
        @gfi.puts "D #{f}"
        @files[@curbranch].delete(f)
      end
    end
    @gfi.puts

    @branchcache[@curbranch] = ":#@mark"

    @mark
  end

  def _command(*args)
    IO.popen('-', 'r') do |io|
      if not io # child
        exec(*args)
      end

      io.read
    end
  end

  def _quote(str)
    if str =~ /[\\\n]/
      '"'+str.gsub(/[\\\n]/) {|chr| "\\"+chr[0].to_s(8)}+'"'
    else
      str
    end
  end

  def _unquote(str)
    if str =~ /^".*"$/
      str[1..-2].gsub(/\\\d\d\d/) {|str| str[1..-1].to_i(8).chr}
    else
      str
    end
  end
end


if $0 == __FILE__
  status = lambda do |str|
    $stderr.puts str
  end

  params = Repo.parseopt([]) {}

  if ARGV.length != 3
    puts "call: togit <cvsroot> <module> <gitdir>"
    exit 1
  end

  cvsdir, modul, gitdir = ARGV

  gitrepo = GitDestRepo.new(gitdir, status)
  cvsrepo = Repo.new(cvsdir, gitrepo, params)
  cvsrepo.status = status
  cvsrepo.scan(modul)
  cvsrepo.commit_sets
end

end     # module FromCVS
