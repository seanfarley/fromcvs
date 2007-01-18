require 'fromcvs'

require 'enumerator'

# We are outputting a git-fast-import stream

class GitDestRepo
  def initialize(gitroot, status=lambda{|s|})
    @status = status

    @gitroot = gitroot
    if not File.stat(@gitroot).directory? or
        not File.stat(File.join(@gitroot, '.git')).directory?
      raise Errono::ENOENT, "dest dir `#@gitroot' is no git repo"
    end

    @deleted = []
    @modified = []
    @mark = 0
    @from = nil
    @branchcache = {}
    @files = Hash.new{|h,k| h[k] = {}}
  end

  def last_date
    log = _command(*%w{git-cat-file -p HEAD})
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
    @gfi = IO.popen('-', 'w')
    if not @gfi   # child
      Dir.chdir(@gitroot)
      exec('git-fast-import')
      $stderr.puts "could not spawn git-fast-import"
      exit 1
    end

    _command(*%w{git-ls-remote -h .}).split("\n").each do |line|
      sha, branch = line.split
      branch[/^.*\//] = ""
      @branchcache[branch] = sha
    end
    @pickupbranches = @branchcache.dup
  end

  def flush
  end

  def has_branch?(branch)
    @branchcache.has_key?(branch)
  end

  # This requires that no commits happen to the parent before
  # we don't commit to the new branch
  def create_branch(branch, parent, vendor_p, date)
    if @branchcache.has_key?(branch)
      raise RuntimeError, "creating existant branch"
    end

    @gfi.puts "reset refs/heads/#{branch}"
    if not vendor_p
      parent ||= 'master'
      @gfi.puts "from #{@branchcache[parent]}"
    end
    @gfi.puts
    @branchcache[branch] = @branchcache[parent]
  end

  def select_branch(branch)
    @curbranch = _quote(branch || 'master')
  end

  def remove(file)
    @deleted << _quote(file)
    @files[@curbranch].delete(file)
  end

  def update(file, data, mode, uid, gid)
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
    @modified << [_quote(file), mode, @mark]
    @files[@curbranch][file] = true
  end

  def commit(author, date, msg)
    _commit(author, date, msg)
  end

  def merge(branch, author, date, msg)
    _commit(author, date, msg, branch)
  end

  def finish
    @gfi.close_write
    raise RuntimeError, "git-fast-import did not succeed" if not $?.success?
  end

  private

  def _commit(author, date, msg, branch=nil)
    @mark += 1
    @gfi.print <<-END
commit refs/heads/#@curbranch
mark :#@mark
committer #{author} <#{author}> #{date.to_i} +0000
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
    @deleted.each do |f|
      @gfi.puts "D #{f}"
    end
    @modified.each do |f, mode, mark|
      @gfi.puts "M #{mode.to_s(8)} :#{mark} #{f}"
    end
    @gfi.puts

    @branchcache[@curbranch] = ":#@mark"
    @deleted = []
    @modified = []
    @from = nil

    @mark
  end

  def _command(*args)
    IO.popen('-', 'r') do |io|
      if not io # child
        Dir.chdir(@gitroot)
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

  if ARGV.length != 3
    puts "call: togit <cvsroot> <module> <gitdir>"
    exit 1
  end

  cvsdir, modul, gitdir = ARGV

  cvsrepo = Repo.new(cvsdir, modul, status)
  gitrepo = GitDestRepo.new(gitdir, status)
  cvsrepo.convert(gitrepo)
end
