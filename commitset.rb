require 'rcsfile'
require 'find'
require 'md5'
require 'sqlite3'

class RCSFile::Rev
  attr_accessor :file, :syms

  # we sort revs on branch, author, log, date
  def <=>(rhs)
    _cmp(rhs) <=> 0
  end

  def _cmp(rhs)
    r = 0
    if (@syms & rhs.syms).empty?
      r = @syms <=> rhs.syms
    end

    for type in [:@author, :@log]
      # scale the res so it doesn't collide with time diffs
      return r * 1000 if r != 0
      r = self.instance_variable_get(type) <=> rhs.instance_variable_get(type)
    end

    @date - rhs.date
  end

  def same_set?(rhs)
    _cmp(rhs).abs < 180
  end
end

class RCSFile
  def branch_syms_of(rev)
    if not @sym_rev
      # We reverse the branches hash
      # but as there might be more than one tag
      # pointing to the same branch, we have to
      # create value arrays.
      # At the same time we ignore any non-branch tag
      @sym_rev = {}
      self.symbols.each_pair do |k, v|
	f = v.split('.')
	f.delete_at(-2) if f[-2] == '0'
	next unless f.length == 3
	@sym_rev[f] = [] unless @sym_rev[f]
	@sym_rev[f].push(k).sort!
      end
    end

    branch = rev.split('.')[0..-2]
    if @sym_rev.key?(branch)
      return @sym_rev[branch]
    else
      return []
    end
  end
end


class Repo
  class Set < Array
    attr_accessor :id, :author, :date, :branch

    def update_id!
      @id = MD5.md5
      self.each do |r|
	@id << r.file << r.rev
      end
    end
  end

  attr_reader :sets

  def initialize(path)
    @path = path
  end

  def _normalize_path(f)
    f = f[@path.length..-1] if f.index(@path) == 0
    f = f[0..-3] if f[-2..-1] == ',v'
    fi = File.split(f)
    fi[0] = File.dirname(fi[0]) if File.basename(fi[0]) == 'Attic'
    return File.join(fi)
  end

  def scan!
    revs = []
    Find.find(@path) do |f|
      next if f[-2..-1] != ',v'
      next if File.directory?(f)

      RCSFile.open(f) do |rf|
	rf.each_value do |rev|
	  rev.file = _normalize_path(f)
	  rev.syms = rf.branch_syms_of(rev.rev)
	  rev.log = MD5.md5(rf.getlog(rev.rev)).to_s
	  revs.push rev
	end
      end
    end

    revs.sort!

    @sets = []
    set = Set.new
    branches = nil
    for r in revs
      if not set.empty? and not set[-1].same_set?(r)
	set.branch = branches[0] if branches
	set.author = set[0].author
	set.date = set[0].date
	@sets << set
	set = Set.new
	branches = nil
      end
      set << r
      if branches
	branches &= r.syms
      elsif not r.syms.empty?
	branches = r.syms.dup
      end
    end
    set.branch = branches[0] if branches
    set.author = set[0].author
    set.date = set[0].date
    @sets << set

    self
  end
end

#SQLite3::Database.open('commitsets.db') do |db|
  r = Repo.new('/space/cvs/dragonfly/src/sys')
  r.scan!

  for s in r.sets
    s.update_id!
    puts "changeset #{s.id} by #{s.author} at #{s.date}" + \
      if s.branch
	" on #{s.branch}"
      else
	""
      end
  end
#end
