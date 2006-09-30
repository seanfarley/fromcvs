require 'rcsfile'
require 'find'
require 'md5'


class RCSFile::Rev
  attr_accessor :file, :syms, :author, :branches, :state, :rev, :next
  attr_accessor :action

  # we sort revs on branch, author, log, date
  def <=>(rhs)
    _cmp(rhs) <=> 0
  end

  def _cmp(rhs)
    r = 0
    if (@syms & rhs.syms).empty?
      r = @syms <=> rhs.syms
      return r * 1000 if r != 0
    end

    for type in [:@author, :@log]
      # scale the res so it doesn't collide with time diffs
      r = self.instance_variable_get(type) <=> rhs.instance_variable_get(type)
      return r * 1000 if r != 0
    end

    @date - rhs.date
  end

  def same_set?(rhs)
    _cmp(rhs).abs < 180
  end

  def branch
    @rev[0..@rev.rindex('.')-1]
  end

  def branch_level
    (@rev.count('.') - 1) / 2
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
    attr_accessor :author, :date

    def <<(rev)
      super

      if rev.syms
	@branches ||= rev.syms
	@branches &= rev.syms
      end
      unless @author
	@author = rev.author
	@date = rev.date
      end
    end

    def push(*args)
      for a in args
	self << a
      end
    end

    def branch
      @branches[0] if @branches
    end
  end

  attr_reader :sets

  def initialize(path, status=nil)
    @status = status || lambda {|m|}
    @path = path.chomp(File::SEPARATOR)
  end

  def _normalize_path(f)
    f = f[@path.length+1..-1] if f.index(@path) == 0
    f = f[0..-3] if f[-2..-1] == ',v'
    fi = File.split(f)
    fi[0] = File.dirname(fi[0]) if File.basename(fi[0]) == 'Attic'
    fi.delete_at(0) if fi[0] == '.'
    return File.join(fi)
  end

  def scan(from_date=Time.at(0))
    # at the expense of some cpu we normalize strings through this
    # hash so that each distinct string only is present one time.
    norm_h = {}
    the_one_and_only_empty_list = [].freeze

    lastdir = nil
    @revs = []
    Find.find(@path) do |f|
      next if f[-2..-1] != ',v'
      next if File.directory?(f)

      dir = File.dirname(f)
      if dir != lastdir
	@status.call(dir)
	lastdir = dir
      end

      next if File.mtime(f) < from_date

      brevs = []
      rh = {}
      nf = _normalize_path(f)
      RCSFile.open(f) do |rf|
	rf.each_value do |rev|
	  # we need to record branch starts so that we can generate
	  # the correct "inverse" next pointer for a later cvs diff
	  if not rev.branches.empty?
	    brevs << rev
	  else
	    rev.branches = the_one_and_only_empty_list
	  end
	  rh[rev.rev] = rev

	  # for old revs we don't need to pimp up the fields
	  # because we will drop it soon anyways
	  #next if rev.date < from_date

	  rev.file = nf
          if rev.branch_level > 0
            rev.syms = rf.branch_syms_of(rev.rev).collect! {|s| norm_h[s] ||= s }
          else
            rev.syms = the_one_and_only_empty_list
          end
	  rev.log = MD5.md5(rf.getlog(rev.rev)).digest
	  rev.author = norm_h[rev.author] ||= rev.author
	  rev.rev = norm_h[rev.rev] ||= rev.rev
	  rev.next = norm_h[rev.next] ||= rev.next
	  rev.state = rev.state.intern
	end

        # if we are on a branch, ignore trunk, as it is unnamed
        if rf.branch
          branchpos = rf.branch[/^\d+\.\d+/]
          rev = rh[rf.head]
          while rev.rev != branchpos
            rev.action = :ignore
            rev = rh[rev.next]
          end
        end

        # special verndor branch handling:
        if rh.has_key?('1.1.1.1')
          # find out if the file was taken off the vendor branch
          trunkdate = nil
          nomerge_vendor = rf.branch != nil && rf.branch != '1.1.1'
          if rf.branch != '1.1.1'
            rev = rh[rf.head]
            while rev.rev != '1.1'
              trunkdate = rev.date
              rev = rh[rev.next]
            end
          end

          # chop off all vendor branch versions since HEAD left the branch
          # of course only if we're not (again) on the branch
          rev = rh['1.1.1.1']
          while rev
            if nomerge_vendor or (trunkdate and rev.date >= trunkdate)
              rev.action = :vendor
            else
              rev.action = :vendor_merge
            end
            rev = rh[rev.next]
          end

          # actually this 1.1 thing is totally in our way, because 1.1.1.1 already
          # covers everything.
          # oh WELL.  old CVS seemed to add a one-second difference
          if (0..1) === rh['1.1.1.1'].date - rh['1.1'].date
            rh['1.1'].action = :ignore
          end
        end

        # correct the next pointers
        for br in brevs
          for rev in br.branches
            # XXX handle versions on nameless branches

            pr = br
            begin
              rev = rh[rev]
              nrev = rev.next
              rev.next = pr.rev
              if rev.branch == rf.branch
                rev.action ||= :branch_merge
              else
                rev.action ||= :branch
              end
              pr = rev
              rev = nrev
            end while rev
          end
          br.branches = the_one_and_only_empty_list
        end

        rh.delete_if { |k, rev| rev.date < from_date }

        @revs += rh.values
      end
    end

    self
  end

  def aggregate
    @status.call("Sorting...")
    @revs.sort!

    @status.call("Aggregating...")
    @sets = []
    set = Set.new
    for r in @revs
      if not set.empty? and not set[-1].same_set?(r)
        set.sort! {|a, b| a.file <=> b.file }
	@sets << set
	set = Set.new
      end
      set << r
    end
    @sets << set if set.author
    @sets.sort! {|a, b| a.date <=> b.date }

    @revs = nil

    self
  end
end

if $0 == __FILE__
  require 'pp'

  r = Repo.new(ARGV[0], lambda {|m| puts m})
  r.scan
  r.aggregate

  r.sets.each do |s|
    puts "#{s.author} #{s.date} on #{s.branch or "trunk"}"
    s.each {|r| puts "\t#{r.file} #{r.rev} #{r.state} #{r.action} #{r.branches.join(',')}"}
  end
end
