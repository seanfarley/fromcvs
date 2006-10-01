require 'rcsfile'
require 'find'
require 'md5'


class RCSFile::Rev
  attr_accessor :file, :syms, :author, :branches, :state, :rev, :next
  attr_accessor :action, :link, :branch_from

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
	next unless f.length % 2 == 1
	@sym_rev[f] ||= []
	@sym_rev[f].push(k).sort!
      end
    end

    branch = rev.split('.')[0..-2]
    return @sym_rev[branch] || []
  end
end


class Repo
  class Set < Array
    attr_accessor :author, :date, :ignore, :branch_from

    def extract_data
      @author = self[0].author
      @date = self[0].date
      @ignore = true

      bl = -1

      each do |rev|
	@branches ||= rev.syms
	@branches &= rev.syms
        if rev.branch_level > bl
          @branch_from = rev.branch_from
        end
        @ignore &&= rev.action == :ignore
      end

      sort! {|a,b| a.file <=> b.file}
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
        trunkrev = nil    # "rev 1.2", when the vendor branch was overwritten

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

          # the quest for trunkrev only happens on trunk (duh)
          if rev.branch_level == 0
            if rev.rev != '1.1'
              trunkrev = rev if not trunkrev or trunkrev.date > rev.date
            end
          end
	end

        # What we need to do to massage the revs correctly:
        # * branch versions need action "branch" and a name assigned
        # * branch versions without a named branch need to be ignored
        # * if the first rev on a level 1-branch/1.1 is "dead", ignore this rev
        # * if we are on a branch
        #   - all trunk revs *above* the branch point are unnamed and
        #     need to be ignored
        #   - all main branch revisions need to be merged
        #   - all branch revisions leading to the main branch need to be merged
        # * if there is a vendor branch
        #   - all vendor revisions older than "rev 1.2" need to be merged
        #   - rev 1.1 needs to be ignored if it happened at the vendor branch time
        #

        # special verndor branch handling:
        if rh.has_key?('1.1.1.1')
          # If the file was added only on the vendor branch, 1.2 is "dead", so ignore
          # those revs completely
          if trunkrev and trunkrev.next == '1.1' and
                trunkrev.state == :dead and trunkrev.date == rh['1.1'].date
            trunkrev.action = :ignore
            rh['1.1'].action = :ignore
          end

          if rf.branch == '1.1.1'
            trunkrev = nil
          end

          # some imports are without vendor symbol.  just fake one up then
          vendor_sym = []
          if rf.branch_syms_of('1.1.1.1').empty?
            vendor_sym = ['FIX_VENDOR']
          end

          # chop off all vendor branch versions since HEAD left the branch
          # of course only if we're not (again) on the branch
          rev = rh['1.1.1.1']
          while rev
            if not trunkrev or rev.date < trunkrev.date
              rev.action = :vendor_merge
            else
              rev.action = :vendor
            end
            rev.syms += vendor_sym
            rev = rh[rev.next]
          end

          # actually this 1.1 thing is totally in our way, because 1.1.1.1 already
          # covers everything.
          # oh WELL.  old CVS seemed to add a one-second difference
          if (0..1) === rh['1.1.1.1'].date - rh['1.1'].date
            rh['1.1'].action = :ignore
          end
        end

        if rf.branch and rf.branch != '1.1.1'
          brsplit = rf.branch.split('.')
          brrev = brsplit[0..1].join('.')

          rev = rh[rf.head]
          while rev.rev != brrev
            rev.action = :ignore
            rev = rh[rev.next]
          end

          level = 1
          loop do
            brrev = brsplit[0..(2 * level)]
            rev = rev.branches.find {|b| b.rev.split('.')[0..(2 * level)] == brrev}
            rev = rh[rev]

            break if brrev == rf.branch

            brrev = brsplit[0..(2 * level + 1)].join('.')
            while rev.rev != brrev
              rev.action = :branch_merge
              rev = rh[rev.next]
            end
            level += 1
          end

          while rev
            rev.action = :branch_merge
            rev = rh[rev.next]
          end
        end

        rh.each_value do |rev|
          if rev.branch_level > 0
            # if it misses a name, ignore
            if rev.syms.empty?
              rev.action = :ignore
            else
              if rev.branch_level == 1
                rev.branch_from = :TRUNK
              else
                # determine the branch we branched from
                br = rev.rev.split('.')[0..-3]
                # if we "branched" from the vendor branch
                # we effectively are branching from trunk
                if br[0..-2] == ['1', '1', '1']
                  rev.branch_from = :TRUNK
                else
                  br = rh[br.join('.')]
                  rev.branch_from = br.syms[0]
                end
              end
            end
            rev.action ||= :branch
          end

          rev.branches.each do |r|
            r = rh[r]
            r.link = rev.rev

            # is it "was added on branch" ...?
            if r.state == :dead and rev.date == r.date
              r.action = :ignore
            end
          end
        end

        # it was added on a different branch!
        rev = rh['1.1']
        if rev and rev.state == :dead     # don't laugh, some files start with 1.2
          rev.action = :ignore
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
        set.extract_data
	@sets << set
	set = Set.new
      end
      set << r
    end
    if not set.empty?
      set.extract_data
      @sets << set
    end
    @sets.sort! {|a, b| a.date <=> b.date }

    @revs = nil

    self
  end
end

if $0 == __FILE__
  require 'pp'

  repo = Repo.new(ARGV[0], lambda {|m| puts m})
  repo.scan
  repo.aggregate

  repo.sets.each do |s|
    print "#{s.author} #{s.date} on "
    if s.branch
      print "#{s.branch} branching from #{s.branch_from}"
    else
      print "trunk"
    end
    if s.ignore
      print " ignore"
    end
    print "\n"
    s.each {|r| puts "\t#{r.file} #{r.rev} #{r.state} #{r.action} #{r.syms.join(',')}"}
  end

  puts "#{repo.sets.length} sets"
end
