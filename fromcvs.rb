require 'rcsfile'
require 'find'
require 'md5'
require 'rbtree'
require 'iconv'
require 'getoptlong'

require 'tagexpander'


module Find
  def find_with_symlinks(*paths, &block)
    find(*paths) do |file|
      block.call(file)
      recurse_paths = []
      begin
        next unless File.lstat(file).symlink? and File.directory?(file)
        Dir.open(file) do |dir|
          dir.each do |f|
            next if f == '.' or f == '..'
            recurse_paths << File.join(file, f)
          end
        end
      rescue Errno::EACCES, Errno::ENOENT
      end
      if recurse_paths
        find_with_symlinks(*recurse_paths, &block)
      end
    end
  end

  module_function :find_with_symlinks
end


module RevSort
  attr_accessor :max_date

  # we sort revs on branch, author, log, date
  def <=>(rhs)
    if @commitid or rhs.commitid
      if not @commitid && rhs.commitid
        # one is unset, we will be different
        r = @commitid ? 1 : -1
      else
        r = @commitid <=> rhs.commitid
        r = cmp_syms(rhs) if r == 0
      end
      r
    else
      _cmp(rhs)
    end
  end

  def cmp_dates(d, l, h)
    l -= 180
    h += 180
    if d.between?(l, h)
      return 0
    else
      return d - l
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

    cmp_syms(rhs)
  end

  def cmp_syms(rhs)
    ls = self.syms
    rs = rhs.syms

    return 0 if !ls && !rs

    ls ||= []
    rs ||= []

    if not (ls & rs).empty?
      0
    else
      ls <=> rs
    end
  end
end

class RCSFile::Rev
  attr_accessor :file, :rcsfile
  attr_accessor :syms, :author, :branches, :state, :rev, :next
  attr_accessor :action, :link, :branch_from
  attr_accessor :commitid

  include RevSort

  def branch
    @rev[0..@rev.rindex('.')-1]
  end

  def branch_level
    (@rev.count('.') - 1) / 2
  end
end

module FromCVS

class Repo

  MAX_TIME_COMMIT = 180

  class Set
    attr_accessor :author, :date, :ignore, :branch_from, :branch_level
    attr_accessor :branch
    attr_accessor :log
    attr_accessor :max_date
    attr_accessor :ary
    attr_accessor :commitid

    include RevSort

    def initialize
      @ary = []
      @ignore = true
      @branch_level = -1
      @syms = {}
    end

    def <<(rev)
      if not @author
        @author = rev.author
        @log = rev.log
        @date = rev.date
        @max_date = rev.date
        @commitid = rev.commitid
      end

      add_syms(rev.syms)

      rev.log = @log    # save memory
      rev.commitid = @commitid

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

    def add_syms(syms)
      return unless syms

      syms.each do |sym|
        @syms[sym] = sym
      end
    end

    def syms
      @syms.keys
    end

    def split_branch(branch)
      new_set = self.dup
      new_set.branch = branch
      new_set.ary = @ary.dup

      changed = false
      new_set.ary.delete_if do |rev|
        if branch
          if !rev.syms || !rev.syms.include?(branch)
            changed = true
          end
        else
          if rev.syms
            changed = true
          end
        end
      end

      return new_set if not changed

      # Okay, something changed, so finish up the fields.
      # We keep the same time like the original changeset to prevent
      # time warps for now.

      # XXX change me when we start keeping ignored revs
      new_set.ignore = true if @ary.empty?

      # Recalc branch pos
      new_set.branch_level = -1
      new_set.ary.each do |rev|
        if rev.branch_level > new_set.branch_level
          new_set.branch_from = rev.branch_from
          new_set.branch_level = rev.branch_level
        end
      end

      new_set
    end
  end

  class BranchPoint
    attr_accessor :name
    attr_accessor :level, :from
    attr_accessor :files, :revs
    attr_accessor :vendor
    attr_reader :state
    attr_accessor :create_date

    STATE_HOLDOFF = 0
    STATE_MERGE = 1
    STATE_BRANCHED = 2

    def initialize(level=-1, from=nil, vendor=false)
      @level = level
      @from = from
      @vendor = vendor
      @files = {}
      @state = STATE_HOLDOFF
      @create_date = Time.at(0)
    end

    def update(bp)
      if @level < bp.level
        @level = bp.level
        @from = bp.from
      end
      @vendor |= bp.vendor
    end

    def holdoff?
      @state == STATE_HOLDOFF
    end

    def merge?
      @state == STATE_MERGE
    end

    def branched?
      @state == STATE_BRANCHED
    end

    def state_transition(dest)
      case dest
      when :created
        @state = STATE_MERGE if holdoff?
      when :branched
        @state = STATE_BRANCHED if merge?
      end
    end
  end

  @@norm_h = Hash.new{|h, k| h[k] = k}

  attr_reader :sets, :sym_aliases, :branchlists, :branchpoints

  attr_accessor :status

  def self.parseopt(addopt)
    opts = GetoptLong.new(
      *([
      [ '--ignore', GetoptLong::REQUIRED_ARGUMENT ],
      [ '--mergesym', GetoptLong::NO_ARGUMENT ]
      ] + addopt)
    )

    params = {}

    opts.each do |opt, arg|
      case opt
      when '--ignore'
        params[:ignore] = arg
      when '--mergesym'
        params[:mergesym] = true
      end

      yield opt, arg    # pass on to caller
    end

    params
  end

  def initialize(cvsroot, destrepo, param={})
    @status = lambda {|m|}
    @cvsroot = cvsroot.dup
    while @cvsroot.chomp!(File::SEPARATOR) do end
    @expander = TagExpander.new(@cvsroot)
    @authormap = Hash.new {|h, k| k}
    begin
      authors = File.read(File.join(@cvsroot, 'CVSROOT', 'authormap'))
      authors.split(/\n/).each do |line|
        author, full = line.split(/[\t ]+/, 2)
        @authormap[author] = full
      end
    rescue
      # oh well, no author map then.
    end
    @destrepo = destrepo
    @from_date = @destrepo.last_date.succ

    # Handling of repo surgery
    if filelist = @destrepo.filelist(:complete)
      @known_files = {}
      @repocopy = Hash.new{|h, k| h[k] = []}
      filelist.each {|v| @known_files[v] = true}
    end

    # branchlists is a Hash mapping parent branches to a list of BranchPoints
    @branchlists = Hash.new {|h, k| h[k] = []}
    @branchpoints = Hash.new {|h, k| h[k] = BranchPoint.new}
    @birthdates = {}
    @sym_aliases = Hash.new {|h, k| h[k] = [k]}
    @sets = MultiRBTree.new

    # branchrevs is a Hash mapping branches to the branch start revs
    @branchrevs = Hash.new {|h, k| h[k] = []}

    if param[:ignore]
      @ignore_branch = Regexp.new(param[:ignore])
    end

    if param[:mergesym]
      @mergesym = true
    end
  end

  def _normalize_path(path, f, prefix=nil)
    f = f[path.length+1..-1] if f.index(path) == 0
    f = f[0..-3] if f[-2..-1] == ',v'
    fi = File.split(f)
    fi[0] = File.dirname(fi[0]) if File.basename(fi[0]) == 'Attic'
    fi.delete_at(0) if fi[0] == '.'
    fi.unshift(prefix) if prefix
    return File.join(fi)
  end

  def scan(modul, prefix=nil)
    modul = modul.dup
    while modul.chomp!(File::SEPARATOR) do end
    path = File.join(@cvsroot, modul)

    lastdir = nil
    Find.find_with_symlinks(path) do |f|
      next if f[-2..-1] != ',v'
      next if File.directory?(f)

      dir = File.dirname(f)
      if dir != lastdir
	@status.call(dir)
	lastdir = dir
      end

      rcsfile = f[@cvsroot.length+1..-1]
      nf = _normalize_path(path, f, prefix)

      if @known_files and not @known_files.member?(nf)
        appeared = true
      else
        appeared = false
      end
      if File.mtime(f) < @from_date and not appeared
        next
      end

      begin
        scan_file(rcsfile, nf, appeared)
      rescue Exception => e
        ne = e.exception(e.message + " while handling RCS file #{rcsfile}")
        ne.set_backtrace(e.backtrace)
        raise ne
      end
    end

    @sym_aliases.values.uniq.each do |syms|
      # collect best match
      # collect branch list
      bp = BranchPoint.new
      bl = []
      syms.each do |sym|
        bp.update(@branchpoints[sym])
        bl.concat(@branchrevs[sym])
        @branchrevs[sym] = bl
      end
      # and write back
      syms.each do |sym|
        @branchpoints[sym] = bp
      end
    end

    @branchpoints.each do |name, bp|
      bp.name ||= name
    end

    @branchrevs.each do |sym, bl|
      sym = @sym_aliases[sym][0]
      bl.uniq!

      next if bl.empty?

      bp = @branchpoints[sym]
      bp.name = sym
      if bp.vendor
        bp.from = nil
      end
      if bp.from
        bp.from = @sym_aliases[bp.from][0]
      end
      bp.revs = Hash[*bl.zip(bl).flatten]
      @branchlists[bp.from] << bp
    end

    # We aggregated all commits with the same author/branch/log into
    # one set.  Now we need to split this set into "real" sets, based
    # on maximum time betweeen dates.
    splitsets = MultiRBTree.new
    splitsets.readjust {|a,b| a.date <=> b.date}
    backlog = []
    while (set = backlog.shift) || (set = @sets.shift and set = set[0])
      next if set.ignore
      # If the set has a commitid, the information is authorative
      if set.commitid
        splitsets[set] = set
        next
      end
      set.ary.sort!{|a,b| a.date <=> b.date}
      last_date = set.ary[0].date
      set.ary.each_with_index do |rev, idx|
        if rev.date > last_date + MAX_TIME_COMMIT
          older_set = Set.new
          newer_set = Set.new
          set.ary[0...idx].each {|r| older_set << r}
          set.ary[idx..-1].each {|r| newer_set << r}
          backlog << newer_set
          set = older_set
          break
        end
        last_date = rev.date
      end
      splitsets[set] = set
    end

    @fixedsets = []
    while set = splitsets.shift and set = set[0]
      if not set.commitid
        # Check if there are multiple revs hitting one file, if so, split
        # the set in two parts.
        dups = []
        set.ary.inject(Hash.new(0)) do |h, rev| 
          # copy the first dup rev and then continue copying
          if h.include?(rev.file) or not dups.empty?
            dups << rev
          end
          h[rev.file] = true
          h
        end

        if not dups.empty?
          # split into two sets, and repopulate them to get times right
          # then queue the later set back so that it is located at the right time
          first_half = set.ary - dups
          set = Set.new
          first_half.each{|r| set << r}
          queued_set = Set.new
          dups.each{|r| queued_set << r}
          splitsets[queued_set] = queued_set
        end
      end

      # Repo copies:  Hate Hate Hate.  We have to deal with multiple
      # almost equivalent branches in one set.
      branches = set.syms.collect {|s| @sym_aliases[s][0]}
      branches.uniq!

      if branches.length < 2
        set.branch = branches[0]
        @fixedsets << set
      else
        branches.each do |branch|
          bset = set.split_branch(branch)
          @fixedsets << bset if not bset.ary.empty?
        end
      end
    end

    self
  end

  def scan_file(rcsfile, nf, appeared)
    rh = {}   # The revision hash

    RCSFile.open(File.join(@cvsroot, rcsfile)) do |rf|
      # Some files are just bare, without any history at all (openoffice)
      # Pretend these files do not exist.
      return if rf.empty?

      trunkrev = nil    # "rev 1.2", when the vendor branch was overwritten

      rf.each_value do |rev|
        rh[rev.rev] = rev

        # for old revs we don't need to pimp up the fields
        # because we will drop it soon anyways
        #next if rev.date < from_date

        rev.file = nf
        rev.rcsfile = rcsfile
        rev.log = Digest::MD5::digest(rf.getlog(rev.rev))
        rev.author = @@norm_h[rev.author]
        rev.rev = @@norm_h[rev.rev]
        rev.next = @@norm_h[rev.next]
        rev.state = rev.state.intern

        # the quest for trunkrev only happens on trunk (duh)
        if rev.branch_level == 0
          if rev.rev != '1.1'
            trunkrev = rev if not trunkrev or trunkrev.date > rev.date
          end
        end
      end

      # We reverse the branches hash
      # but as there might be more than one tag
      # pointing to the same branch, we have to
      # create value arrays.
      # At the same time we ignore any non-branch tag
      sym_rev = {}
      rf.symbols.each_pair do |k, v|
        # ignore branches
        next if @ignore_branch and @ignore_branch =~ k

        vs = v.split('.')
        if vs[-2] == '0'
          magic = 1
          bp = vs[0..-3] + vs[-1,1]
        else
          magic = 0
          bp = vs
        end
        if vs.length % 2 == magic || vs.length < 2
          next
        end
        # If the parent doesn't exist, the symbol is bogus
        if not rh[bp[0..-2].join('.')]
          $stderr.puts "#{rcsfile}: branch symbol `#{k}' has dangling revision #{v}"
          next
        end
        sym_rev[bp] ||= []
        sym_rev[bp] << @@norm_h[k]
        sym_rev[bp].sort!
      end

      sym_rev.each do |rev, sl|
        bp = rev[0..-2]

        vendor = false
        level = bp.length / 2
        # if we "branched" from the vendor branch in the past
        # we were effectively branching from trunk.  Adjust
        # for this fact.
        if ['1.1.1', rf.branch].include?(bp[0,3].join('.'))
          level -= 1
          if ['1.1.1', rf.branch].include?(rev.join('.'))
            vendor = true
          end
        end

        parentname = nil
        if level > 1
          bprev = rh[bp.join('.')]
          # the parent branch doesn't have a name, so we don't know
          # where to branch off in the first place.
          # I'm not sure what to do exactly, but for now hope that
          # another file will still have the branch name.
          next if not bprev

          parentbranch = bp[0..-2]
          parentname = sym_rev[parentbranch]
          next if not parentname
          parentname = parentname[0]
        end

        bpl = @branchpoints[sl[0]]
        # if level > bpl.level
        #   puts "upgrading #{sl[0]} to #{level}/#{parentname} (#{rev.join('.')})"
        # end
        bpl.update(BranchPoint.new(level, parentname, vendor))

        if sl.length > 1 and @mergesym
          # record symbol aliases, merge with existing
          sl2 = sl.dup
          sl.each do |s|
            sl2 += @sym_aliases[s]
          end
          sl2.uniq!
          sl2.sort!
          # if sl2 != @sym_aliases[sl2[0]]
          #   puts "#{rcsfile} aliases #{sl.join(',')} to #{sl2.join(',')}"
          # end
          sl2.each {|s| @sym_aliases[s] = sl2}
        end

        # Branch handling
        # Unfortunately branches can span multiple changesets.  To fix this,
        # we collect the list of branch revisions (revisions where branch X
        # branches from) per branch.
        # When committing revisions, we hold off branching (and record the
        # files) until we are about to change a file on the parent branch,
        # which should already exist in the child branch, or until we are about
        # to commit the first revision to the child branch.  
        # After this, we merge each branch revision to the branch until the
        # list is empty.
        #
        # Add this rev unless the first rev "was added on branch"
        # (will be :ignored later)

        br = rh[bp.join('.')]
        # get first rev of branch
        frev = rh[br.branches.find {|r| r.split('.')[0..-2] == rev}]
        if not frev or frev.date != br.date or frev.state != :dead
          @branchrevs[sl[0]] << br
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
        if rf.branch == '1.1.1'
          trunkrev = nil
        end

        # If the file was added only on the vendor branch, 1.2 is "dead", so ignore
        # those revs completely
        if trunkrev and trunkrev.next == '1.1' and
              trunkrev.state == :dead and trunkrev.date == rh['1.1'].date
          trunkrev.action = :ignore
          rh['1.1'].action = :ignore
        end

        # some imports are without vendor symbol.  just fake one up then
        vendor_sym = sym_rev[['1','1','1']]
        vendor_sym ||= ['FIX_VENDOR']

        # chop off all vendor branch versions since HEAD left the branch
        # of course only if we're not (again) on the branch
        rev = rh['1.1.1.1']
        while rev
          if !trunkrev || rev.date < trunkrev.date
            rev.action = :vendor_merge
          else
            rev.action = :vendor
          end
          rev.syms ||= vendor_sym
          rev = rh[rev.next]
        end

        # actually this 1.1 thing is totally in our way, because 1.1.1.1 already
        # covers everything.
        # oh WELL.  old CVS seemed to add a one-second difference
        if (0..1) === rh['1.1.1.1'].date - rh['1.1'].date
          # TODO: check log messages, somtimes the import log is empty
          rh['1.1'].action = :ignore
        end
      end

      # Handle revs on tunk, when we are actually using a branch as HEAD
      if rf.branch
        brsplit = rf.branch.split('.')
        brrev = brsplit[0..1].join('.')

        rev = rh[rf.head]
        while rev.rev != brrev
          rev.action = :ignore
          rev = rh[rev.next]
        end
      end

      if rf.branch and rf.branch != '1.1.1'
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
          branch = rev.rev.split('.')[0..-2]
          rev.syms = sym_rev[branch]

          # if it misses a name, ignore
          if not rev.syms
            rev.action = :ignore
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

      # record when the file was introduced, we need this later
      # to check if this file even existed when a branch happened
      @birthdates[nf] = (rev || trunkrev).date

      # This file appeared since the last scan/commit
      # If the first rev is before that, it must have been repo copied
      if appeared
        firstrev = rh[rf.head]
        copyrev = firstrev if firstrev.date < @from_date
        while firstrev.next
          firstrev = rh[firstrev.next]
          if firstrev.date < @from_date and not copyrev
            copyrev = firstrev
          end
        end
        if firstrev.date < @from_date
          # This file has been repo copied.  Make up a repo
          # copy revision for each branch which was alive at time
          # of the copy.
          @repocopy[nil] << copyrev
          prepare_file_rev(rf, copyrev) if @destrepo.revs_per_file
          sym_rev.each do |r, syms|
            br = r[0..-2].join('.')   # get parent rev
            # get branch start
            br = rh[br]
            br2 = br.branches.select{|brr| brr.split('.')[0..-2] == r}[0]
            if rh[br2]                # branch might be latent
              br = rh[br2]

              copyrev = br
              while br.next
                br = rh[br.next]
                if br.date < @from_date
                  copyrev = br
                end
              end
            else
              copyrev = br
            end

            next if not copyrev.date < @from_date

            syms.each do |sym|
              @repocopy[@sym_aliases[sym][0]] << copyrev
              prepare_file_rev(rf, copyrev) if @destrepo.revs_per_file
            end
          end
        end
      end

      rh.delete_if { |k, rev| rev.date < @from_date }

      revs = rh.values
      if @destrepo.revs_per_file
        # Create a stable sort order.  this helps the git
        # destination to compress more efficently.
        # At the same time, sort by branch, so that we ideally get
        # uninterrupted runs of history, thus helping compression.
        revs.sort! do |a,b|
          a = a.rev.split('.').collect{|i| i.to_i}
          b = b.rev.split('.').collect{|i| i.to_i}
          a <=> b
        end
      end
      revs.each do |r|
        set = @sets[r]
        if not set
          set = Set.new
          set << r
          @sets[set] = set
        else
          set << r
        end

        if @destrepo.revs_per_file
          prepare_file_rev(rf, r)
        end
      end
    end
  end

  def convtoutf8(str)
    encs = ['utf-8', 'iso8859-15']

    encs.each do |enc|
      begin
        return Iconv::conv('utf-8', enc, str)
      rescue StandardError
      end
    end

    raise RuntimeError, "cannot convert string to utf-8"
  end

  def fixup_branch_before(bp, date)
    return if not bp.holdoff?

    bp.state_transition(:created)

    return if @destrepo.has_branch?(bp.name)

    bp.create_date = date
    @destrepo.create_branch(bp.name, bp.from, false, date)

    # Remove files not (yet) present on the branch
    delfiles = @destrepo.filelist(bp.from).select {|f| not bp.files.include? f}
    return if delfiles.empty?

    @destrepo.select_branch(bp.name)
    revs = []
    delfiles.each do |f|
      rev = RCSFile::Rev.new
      rev.file = f
      @destrepo.remove(f, rev)
      revs << rev
    end

    message = "Removing files not present on branch #{bp.name}:\n\t" +
              delfiles.sort.join("\n\t")
    @destrepo.commit(@authormap['branch-fixup'], date, message, revs)
  end

  def fixup_branch_after(branch, commitid, set)
    # If neccessary, merge parent branch revs to the child branches
    # We need to recurse to reach all child branches
    cleanbl = false
    @branchlists[branch].each do |bp|
      commitrevs = set.ary.select {|rev| bp.revs.include? rev}
      next if commitrevs.empty?

      commitrevs.each do |rev|
        bp.revs.delete(rev)
        bp.files[rev.file] = rev.file
      end
      merging = bp.merge?

      if bp.revs.empty?
        bp.state_transition(:branched)
        cleanbl = true
      end

      # Only bother if we are merging
      next unless merging

      @destrepo.select_branch(bp.name)

      # If this file was introduced after the branch, we are dealing
      # with a FIXCVS issue which was addressed only recently.
      # Previous CVS versions just added the tag to the current HEAD
      # revision and didn't insert a dead revision on the branch with
      # the same date, like it is happening now.
      # This means history is unclear as we can't reliably determine
      # if the tagging happened at the same time as the addition to
      # the branch.  For now, just assume it did.
      commitrevs.reject!{|rev| @birthdates[rev.file] > bp.create_date}
      next if commitrevs.empty?

      message = "Add files from parent branch #{branch || 'HEAD'}:\n\t" +
                commitrevs.collect{|r| r.file}.sort.join("\n\t")
      parentid = commit('branch-fixup', set.max_date, commitrevs,
                 message, commitid)

      cleanbl |= fixup_branch_after(bp.name, parentid, set)
    end

    cleanbl
  end

  def record_holdoff(branch, set)
    @branchlists[branch].each do |bp|
      # if we're holding off, record the files which match the revs
      next unless bp.holdoff?

      set.ary.each do |rev|
        next unless bp.revs.include?(rev)
        bp.revs.delete(rev)
        bp.files[rev.file] = rev.file
      end

      #puts "holding off branch #{bp.name} from #{branch}"
      #set.ary.each do |rev|
      #  puts "\t#{rev.file}:#{rev.rev}"
      #end

      # We have to recurse on all child branches as well
      record_holdoff(bp.name, set)
    end
  end

  def create_branch(bp, date)
    # Recurse to create parents before.
    # I guess this is not strictly necessary, as we are
    # not able to deduce child branches from unbranched
    # parents (laying on the same revs), but hey.
    create_branch(@branchpoints[bp.from], date) if bp.from
    fixup_branch_before(bp, date)
  end

  def commit(author, date, revs, logmsg=nil, mergeid=nil)
    if logmsg.respond_to?(:call)
      logproc = logmsg
      logmsg = nil
    end

    catch :out do
      revs.each do |rev|
        filename = File.join(@cvsroot, rev.rcsfile)

        RCSFile.open(filename) do |rf|
          logmsg = rf.getlog(rev.rev) unless logmsg

          throw :out if not @destrepo.revs_with_cset

          prepare_file_rev(rf, rev)
        end
      end
    end

    logmsg = convtoutf8(logmsg)

    if logproc
      logmsg = logproc.call(logmsg)
    end

    if mergeid
      @destrepo.merge(mergeid, @authormap[author], date, logmsg, revs)
    else
      @destrepo.commit(@authormap[author], date, logmsg, revs)
    end
  end
  private :commit

  def prepare_file_rev(rf, rev)
    if rev.state == :dead
      @destrepo.remove(rev.file, rev)
    else
      data = rf.checkout(rev.rev)
      @expander.expand!(data, rf.expand, rev)
      stat = File.stat(File.join(@cvsroot, rev.rcsfile))
      @destrepo.update(rev.file, data, stat.mode, stat.uid, stat.gid, rev)
      data.replace ''
    end
  end

  def record_incremental_holdoff(branch, files)
    @branchlists[branch].each do |bp|
      next unless bp.holdoff?

      bf = bp.revs.keys.collect{|r| r.file if r.date >= @from_date}

      files.each do |f|
        if not bf.include?(f)
          bp.files[f] = f
        end
      end

      record_incremental_holdoff(bp.name, files)
    end
  end

  def commit_sets
    @destrepo.start

    # Prime data structures so that we know which files already exist on a branch
    (@branchpoints.keys + [nil]).each do |bn|
      record_incremental_holdoff(bn, @destrepo.filelist(bn))
    end

    # XXX First handle possible repo surgery
    if @repocopy
      @repocopy.each do |branch, revs|
        revs.reject!{|r| r.state == :dead}
        next if revs.empty?

        @destrepo.select_branch(branch)
        commit("repo-copy", @from_date, revs, "Repo copy files")
      end
    end

    lastdate = Time.at(0)

    totalsets = @fixedsets.length
    setnum = 0

    while set = @fixedsets.shift
      begin
        setnum += 1
        next if set.ignore

        if setnum % 100 == 0
          @status.call("committing set #{setnum}/#{totalsets}")
        end

        # if we're in a period of silence, tell the target to flush
        if set.date - lastdate > 180
          @destrepo.flush
        end
        if lastdate < set.max_date
          lastdate = set.max_date
        end

        if set.branch
          bp = @branchpoints[set.branch]

          if set.ary.find{|r| [:vendor, :vendor_merge].include?(r.action)}
            if bp.holdoff? and not @destrepo.has_branch?(set.branch)
              @destrepo.create_branch(set.branch, nil, true, set.max_date)
            end
            bp.state_transition(:created)
          else
            fixup_branch_before(bp, set.max_date)
          end
        end

        # Find out if one of the revs we're going to commit breaks
        # the holdoff state of a child branch.
        @branchlists[set.branch].each do |bp|
          next unless bp.holdoff?
          if set.ary.find {|rev| bp.files.include? rev.file}
            fixup_branch_before(bp, set.max_date)
          end
        end

        @destrepo.select_branch(set.branch)
        curbranch = set.branch

        # We commit with max_date, so that later the backend
        # is able to tell us the last point of silence.
        commitid = commit(set.author, set.max_date, set.ary)

        merge_revs = set.ary.select{|r| [:branch_merge, :vendor_merge].include?(r.action)}
        if not merge_revs.empty?
          @destrepo.select_branch(nil)
          curbranch = nil

          commit(set.author, set.max_date, merge_revs,
                     lambda {|m| "Merge from vendor branch #{set.branch}:\n#{m}"},
                     commitid)
        end

        record_holdoff(curbranch, set)

        if fixup_branch_after(curbranch, commitid, set)
          @branchlists.each do |psym, bpl|
            bpl.delete_if {|bp| bp.branched?}
          end
        end
      rescue Exception => e
        ne = e.exception(e.message + " while handling set [%s]" %
                         set.ary.collect{|r| "#{r.rcsfile}:#{r.rev}"}.join(','))
        ne.set_backtrace(e.backtrace)
        raise ne
      end
    end

    # There might be branches around which were created just recently and thus
    # didn't experience a fixup_branch_before.  We have to do this now before
    # we stop running.
    @branchpoints.each_value do |bp|
      create_branch(bp, lastdate)
    end

    @destrepo.finish
  end
end


class PrintDestRepo
  attr_reader :revs_with_cset
  attr_reader :revs_per_file

  def initialize(t)
    @branches = {}
    @t = t
  end

  def start
  end

  def flush
  end

  def has_branch?(branch)
    @branches.include? branch
  end

  def last_date
    @t
  end

  def filelist(branch)
    []
  end
  
  def create_branch(branch, parent, vendor_p, date)
    if vendor_p
      puts "Creating vendor branch #{branch}"
    else
      puts "Branching #{branch} from #{parent or 'HEAD'}"
    end
    @branches[branch] = true
  end

  def select_branch(branch)
    @curbranch = branch
  end

  def remove(file, rev)
    #puts "\tremoving #{file}"
  end

  def update(file, data, mode, uid, gid, rev)
    #puts "\t#{file} #{mode}"
  end

  def _commit(author, date, msg, revs)
    "by %s on %s on %s revs [%s]" %
      [author, date, @curbranch || 'HEAD', revs.map{|e| "#{e.rcsfile}:#{e.rev}"}.join(' ')]
  end

  def commit(author, date, msg, revs)
    puts "set %s" % _commit(author, date, msg, revs)
    @curbranch
  end

  def merge(branch, author, date, msg, revs)
    puts "merge set from %s %s" % [branch, _commit(author, date, msg, revs)]
    @curbranch
  end

  def finish
  end
end

if $0 == __FILE__
  require 'time'

  starttime = Time.at(0)
  param = Repo.parseopt([
      [ '--time', GetoptLong::REQUIRED_ARGUMENT ],
  ]) do |opt, arg|
    starttime = Time.parse(arg)
  end

  printrepo = PrintDestRepo.new(starttime)
  repo = Repo.new(ARGV[0], printrepo, param)
  repo.status = lambda {|m| puts m}

  repo.scan(ARGV[1])

  repo.sym_aliases.sort.each do |sym, a|
    next if [sym] == a
    puts "#{sym} is equivalent to #{a.join(',')}"
  end

  repo.branchlists.each do |parent, l|
    puts "%s is parent of %s" %
      [parent || 'HEAD', l.map{|i| i.name}.join(',')]
  end

  repo.branchpoints.sort{|a,b| a[0]<=>b[0]}.each do |name, bp|
    if bp.vendor
      puts '%s is a vendor branch' % bp.name
    else
      puts '%s branches from %s (%d revs, level %d)' %
        [bp.name,
         bp.from || 'HEAD',
         bp.revs ? bp.revs.length : 0,
         bp.level]
    end
  end

  repo.commit_sets

  exit 0
end

end   # module FromCVS

