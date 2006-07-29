require 'rcsfile'
require 'find'
require 'md5'

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

revs = []

Find.find('/space/cvs/dragonfly/src/sys') do |f|
  next if File.directory?(f)

  RCSFile.open(f) do |rf|
    rf.each_value do |rev|
      rev.file = f
      rev.syms = rf.branch_syms_of(rev.rev)
      rev.log = MD5.md5(rf.getlog(rev.rev)).to_s
      revs.push rev
    end
  end
end

revs.sort!

set = []
for r in revs
  if set.empty? or set[-1].same_set?(r)
    set << r
  else
    puts "commitset by #{set[0].author} at #{set[0].date}:"
    for sr in set
      puts "\t#{sr.file} #{sr.rev} #{sr.date} #{sr.syms}"
    end
    set = [r]
  end
  ro = r
end
