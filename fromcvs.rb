require 'rcsfile'
require 'find'
require 'md5'
require 'rbtree'


module RevSort
  attr_accessor :max_date

  # we sort revs on branch, author, log, date
  def <=>(rhs)
    r = _cmp(rhs)
    return r if r != 0

    def cmp_dates(d, l, h)
      l -= 180
      h += 180
      if d.between?(l, h)
        return 0
      else
        return d - l
      end
    end

    if @max_date
      return - cmp_dates(rhs.date, @date, @max_date)
    else
      return cmp_dates(@date, rhs.date, rhs.max_date || rhs.date)
    end
  end

  def _cmp(rhs)
    ls = @log
    rs = rhs.log
    r = ls <=> rs
    return r if r != 0

    ls = @author
    rs = rhs.author
    r = ls <=> rs
    return r if r != 0

    ls = @syms
    rs = rhs.syms

    if !ls && !rs || (ls or []) & (rs or [])
      r = 0
    else
      r = (ls or []) <=> (rs or [])
    end
    r
  end
end

class RCSFile::Rev
  attr_accessor :file, :syms, :author, :branches, :state, :rev, :next
  attr_accessor :action, :link, :branch_from

  include RevSort

  def branch
    @rev[0..@rev.rindex('.')-1]
  end

  def branch_level
    (@rev.count('.') - 1) / 2
  end
end


class Repo
  class Set
    attr_accessor :author, :date, :ignore, :branch_from
    attr_accessor :syms, :log
    attr_accessor :max_date

    include RevSort

    def initialize
      @ary = []
      @ignore = true
      @branch_level = -1
    end

    def <<(rev)
      if not @author
        @author = rev.author
        @log = rev.log
        @syms = rev.syms
        @date = rev.date
        @max_date = rev.date
      end

      rev.log = @log    # save memory

      if @date > rev.date
        @date = rev.date
      end
      if @max_date < rev.date
        @max_date = rev.date
      end
      if rev.branch_level > @branch_level
        @branch_from = rev.branch_from
        @branch_level = rev.branch_level
      end
      ignore = rev.action == :ignore
      @ignore &&= ignore

      @ary << rev unless ignore
    end

    def each(&block)
      @ary.each(&block)
      self
    end

    def [](idx)
      @ary[idx]
    end
  end

  attr_reader :sets, :sym_aliases

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

    @sym_aliases = Hash.new {|h, k| h[k] = [k]}

    @sets = RBTree.new

    lastdir = nil
    Find.find(@path) do |f|
      next if f[-2..-1] != ',v'
      next if File.directory?(f)

      dir = File.dirname(f)
      if dir != lastdir
	@status.call(dir)
	lastdir = dir
      end

      next if File.mtime(f) < from_date

      rh = {}
      nf = _normalize_path(f)
      RCSFile.open(f) do |rf|
        trunkrev = nil    # "rev 1.2", when the vendor branch was overwritten

        # We reverse the branches hash
        # but as there might be more than one tag
        # pointing to the same branch, we have to
        # create value arrays.
        # At the same time we ignore any non-branch tag
        sym_rev = {}
        rf.symbols.each_pair do |k, v|
          f = v.split('.')
          f.delete_at(-2) if f[-2] == '0'
          next unless f.length % 2 == 1
          sym_rev[f] ||= []
          sym_rev[f].push(norm_h[k] ||= k)
        end
        sym_rev.each_value do |sl|
          next unless sl.length > 1

          # record symbol aliases, merge with existing
          sl2 = sl.dup
          sl.each do |s|
            sl2 += @sym_aliases[s]
          end
          sl2.uniq!
          sl2.each {|s| @sym_aliases[s].replace(sl2)}
        end

	rf.each_value do |rev|
	  rh[rev.rev] = rev

	  # for old revs we don't need to pimp up the fields
	  # because we will drop it soon anyways
	  #next if rev.date < from_date

	  rev.file = nf
          if rev.branch_level > 0
            branch = rev.rev.split('.')[0..-2]
            rev.syms = sym_rev[branch]
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
          vendor_sym = nil
          if not sym_rev[['1','1','1']]
            vendor_sym = ['FIX_VENDOR']
          end

          # chop off all vendor branch versions since HEAD left the branch
          # of course only if we're not (again) on the branch
          rev = rh['1.1.1.1']
          while rev
            if rev.state != :dead && (!trunkrev || rev.date < trunkrev.date)
              rev.action = :vendor_merge
            else
              rev.action = :vendor
            end
            if vendor_sym
            rev.syms ||= vendor_sym
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
            rev = rev.branches.find {|b| b.split('.')[0..(2 * level)] == brrev}
            rev = rh[rev]

            break if brrev.join('.') == rf.branch

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
            if not rev.syms
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
                  rev.branch_from = br.syms
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

        rh.each_value do |r|
          set = @sets[r]
          if not set
            set = Set.new
            set << r
            @sets[set] = set
          else
            set << r
          end
        end
      end
    end

    @sets.readjust {|s1, s2| s1.date <=> s2.date}

    self
  end

  def commit(dest)
    @sets.each_value do |set|
      next if set.ignore

      logmsg = nil

      if set.syms
        # TODO select correct alias
        branch = set.syms[0]

        is_vendor = [:vendor, :vendor_merge].include?(set[0].action)

        if not dest.has_branch?(branch)
          dest.create_branch(branch, set.branch_from, is_vendor)
        end
        dest.select_branch(branch)
      else
        dest.select_branch(nil)
      end

      files = []
      merge_files = []
      set.each do |rev|
        files << rev.file

        filename = File.join(@path, rev.file+',v')
        if not File.exists?(filename)
          fnp = File.split(rev.file+',v')
          filename = File.join(@path, fnp[0..-2], 'Attic', fnp[-1])
        end
 
        RCSFile.open(filename) do |rf|
          logmsg = rf.getlog(rev.rev) unless logmsg

          stat = File.stat(filename)
          if rev.state == :dead
            dest.remove(rev.file)
          else
            data = rf.checkout(rev.rev)
            # implement keyword expansion here
            dest.update(rev.file, data, stat.mode, stat.uid, stat.gid)
          end

          if [:branch_merge, :vendor_merge].include?(rev.action)
            merge_files << [rev.state, rev.file, data, stat.mode, stat.uid, stat.gid]
          end
        end
      end

      commitid = dest.commit(set.author, set.date, logmsg, files)

      unless merge_files.empty?
        dest.select_branch(nil)
        merge_files.each do |p|
          if p.shift == :dead
            dest.remove(p.shift)
          else
            dest.update(*p)
          end
        end

        dest.merge(commitid, set.author, set.date, logmsg, merge_files)
      end
    end
  end
end


class PrintDestRepo
  def initialize
    @branches = {}
  end

  def has_branch?(branch)
    @branches.include? branch
  end

  def create_branch(branch, parent, vendor_p)
    if vendor_p
      puts "Creating vendor branch #{branch}"
    else
      puts "Branching #{branch} from #{parent}"
    end
    @branches[branch] = true
  end

  def select_branch(branch)
    @curbranch = branch
  end

  def remove(file)
    puts "\tremoving #{file}"
  end

  def update(file, data, mode, uid, gid)
    puts "\t#{file} #{mode}"
  end

  def commit(author, date, msg, files)
    puts "set by #{author} on #{date} on #{@curbranch or 'TRUNK'}"
    @curbranch
  end

  def merge(branch, author, date, msg, files)
    puts "merge set from #{branch} by #{author} on #{date} on #{@curbranch or 'TRUNK'}"
  end
end


if $0 == __FILE__
  repo = Repo.new(ARGV[0], lambda {|m| puts m})
  repo.scan
  printrepo = PrintDestRepo.new

  repo.commit(printrepo)

  exit 0

  repo.sets.each_value do |s|
    print "#{s.author} #{s.date} on "
    if s.syms
      print "#{s.syms[0]} branching from #{s.branch_from}"
    else
      print "trunk"
    end
    if s.ignore
      print " ignore"
    end
    print "\n"
    s.each do |r|
      print "\t#{r.file} #{r.rev} #{r.state} #{r.action}"
      print " " + r.syms.join(',') if r.syms
      print "\n"
    end
  end

  repo.sym_aliases.each do |sym, a|
    puts "#{sym} is equivalent to #{a.join(',')}"
  end

  puts "#{repo.sets.length} sets"
end
