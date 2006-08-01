# == Synopsis
#
# commitset: outputs a CVS changeset from a pre-built database
#
# == Usage
#
# commitset [-bhN] [-B path] [-D dbfile] file rev
#
# -b, --build-incr:
#    Incrementally updates the database.
#
# -B path, --build-new path:
#    Builds a database from the CVS repo rooted at +path+.
#
# -D dbfile, --db dbfile:
#    Specifies the location of the database, instead of using
#    +commits.db+ in the current directory.
#
# -N, --nodiff:
#    Just output the files and revisions instead of a complete diff.
#
# -h, --help:
#    This help.
#
#
# commitset will search for the specified +rev+ in +file+ and output a
# diff spanning all associated files and revisions.
#

require 'rcsfile'
require 'find'
require 'md5'
require 'sqlite3'
require 'getoptlong'
require 'rdoc/usage'

# Fix up SQLite3
module SQLite3
  class Database
    def Database.open(*args)
      db = Database.new(*args)
      if block_given?
	begin
	  yield db
	ensure
	  db.close
	end
      else
	return db
      end
    end
  end
end

class RCSFile::Rev
  attr_accessor :file, :syms, :author, :branches, :state, :rev, :next

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
    attr_accessor :id, :author, :date

    def update_id!
      @id = MD5.md5
      self.each do |r|
	@id << r.file << r.rev
      end
    end

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

  def initialize(path)
    @path = path
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

    lastdir = nil
    @revs = []
    Find.find(@path) do |f|
      next if f[-2..-1] != ',v'
      next if File.directory?(f)

      dir = File.dirname(f)
      if dir != lastdir
	yield dir if block_given?
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
	    rev.branches = nil
	  end
	  rh[rev.rev] = rev

	  # for old revs we don't need to pimp up the fields
	  # because we will drop it soon anyways
	  next if rev.date < from_date

	  rev.file = nf
	  rev.syms = rf.branch_syms_of(rev.rev).collect! {|s| norm_h[s] ||= s }
	  rev.log = MD5.md5(rf.getlog(rev.rev)).digest
	  rev.author = norm_h[rev.author] ||= rev.author
	  rev.rev = norm_h[rev.rev] ||= rev.rev
	  rev.next = norm_h[rev.next] ||= rev.next
	  rev.state = nil
	  @revs << rev
	end
      end

      # correct the next pointers
      for br in brevs
	for rev in br.branches
	  pr = br
	  begin
	    rev = rh[rev]
	    nrev = rev.next
	    rev.next = pr.rev
	    pr = rev
	    rev = nrev
	  end while rev
	end
	br.branches = nil
      end
    end

    self
  end

  def aggregate
    yield "Sorting..." if block_given?
    @revs.sort!

    yield "Aggregating..." if block_given?
    @sets = []
    set = Set.new
    for r in @revs
      if not set.empty? and not set[-1].same_set?(r)
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


class Commitset
  def initialize(dbfile, create=false)
    if not create and not File.exists?(dbfile)
      raise Errno::ENOENT, dbfile
    end
    @db = SQLite3::Database.open(dbfile)
    if not create
      @path = @db.get_first_value('SELECT value FROM meta WHERE key = "path"')
    end
  end

  def _init_schema
    @db.execute_batch(%{
      CREATE TABLE IF NOT EXISTS cset (
	cset_id INTEGER PRIMARY KEY,
	branch TEXT,
	author TEXT NOT NULL,
	date INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS file (
	file_id INTEGER PRIMARY KEY,
	path TEXT NOT NULL UNIQUE
      );

      CREATE TABLE IF NOT EXISTS rev (
	file_id INTEGER NOT NULL,
	rev TEXT NOT NULL,
	nrev TEXT,
	cset_id INTEGER NOT NULL,
	PRIMARY KEY ( file_id, rev )
      );

      CREATE INDEX IF NOT EXISTS rev_cset ON rev ( cset_id );

      CREATE TABLE IF NOT EXISTS meta (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
      );

      -- make SQLite3 happy, it checks something it shouldn't
      -- so give it a result to chew on.
      SELECT 1;
    })
  end

  def build(rebuild)
    if (rebuild)
      @db.execute_batch(%{
	DROP TABLE IF EXISTS cset;
	DROP TABLE IF EXISTS file;
	DROP INDEX IF EXISTS rev_cset;
	DROP TABLE IF EXISTS rev;
	DROP TABLE IF EXISTS meta;

	SELECT 1;
      })
      _init_schema
      @db.execute('INSERT INTO meta VALUES ( "path", ? )', rebuild)
      @path = rebuild
      from_date = Time.at(0)
    else
      from_date = @db.get_first_value('SELECT date, cset_id FROM cset 
				         ORDER BY cset_id DESC LIMIT 1')
      from_date = Time.at(from_date.to_i)
    end

    r = Repo.new(@path)
    r.scan(from_date) {|s| yield s if block_given? }
    r.aggregate {|s| yield s if block_given? }

    # we might be operating incrementally and multiple changesets
    # have the same date.  Strip away the ones we already committed.
    while not r.sets.empty?
      s = r.sets[0]
      if s.branch and @db.get_first_value(
	    %{SELECT cset_id FROM cset WHERE
		branch = :branch AND
		author = :author AND
		date = :date
	     }, ':branch' => s.branch,
		':author' => s.author,
		':date' => s.date.to_i
	  ) or @db.get_first_value(
	    %{SELECT cset_id FROM cset WHERE
		branch IS NULL AND
		author = :author AND
		date = :date
	     }, ':author' => s.author,
		':date' => s.date.to_i
	  ) then
	r.sets.delete_at(0)
      else
	break
      end
    end

    begin
      @db.transaction

      n = 0
      ls = r.sets[0] if not r.sets.empty?
      for s in r.sets
	# To handle date issues, we only flush the database in periods of silence
	if ls[-1].date < s.date
	  @db.commit
	  @db.transaction
	end
	ls = s

	if block_given?
	  n += 1
	  yield "#{n}/#{r.sets.length} " + _chsetstr(s.author, s.branch, s.date)
	end

	@db.execute('INSERT INTO cset VALUES ( NULL, :branch, :author, :date )',
		    ':branch' => s.branch,
		    ':author' => s.author,
		    ':date' => s.date.to_i
		   )
	cset_id = @db.last_insert_row_id
	for rev in s
	  fid = @db.get_first_value('SELECT file_id FROM file WHERE path = ?', rev.file)
	  unless fid
	    @db.execute('INSERT INTO file VALUES ( NULL, ? )', rev.file)
	    fid = @db.last_insert_row_id
	  end
	  @db.execute('INSERT INTO rev VALUES ( :fid, :rev, :nrev, :cset_id )',
		      ':fid' => fid,
		      ':rev' => rev.rev,
		      ':nrev' => rev.next,
		      ':cset_id' => cset_id
		     )
	end
	
      end
      @db.commit
    rescue
      @db.rollback
    end
  end

  def _chsetstr(author, branch, date)
    %{Changeset by #{author}#{%{ on #{branch}} if branch} at #{date}}
  end

  def cset(file, rev, diff=true)
    r = ""

    cset_id, branch, author, date = @db.get_first_row(%{
      SELECT * FROM cset WHERE cset_id = (
	SELECT cset_id FROM rev WHERE file_id = (
	  SELECT file_id FROM file WHERE path = :path
	) AND rev = :rev
      )
    }, ':path' => file, ':rev' => rev)
    raise RuntimeError, 'File or revision not found' unless cset_id
    date = Time.at(date.to_i)

    r += _chsetstr(author, branch, date) + "\n"

    rcsf = File.join(@path, file) + ',v'
    begin
      RCSFile.open(rcsf) do |rf|
	r += rf.getlog(rev) + "\n"
      end
    rescue Errno::ENOENT
      pc = File.split(rcsf)
      pc.insert(-2, 'Attic')
      rcsf = File.join(pc)
      retry
    end

    rows = @db.execute('SELECT path, rev, nrev FROM rev NATURAL JOIN file WHERE cset_id = ?',
		       cset_id)
    r += '['
    for path, rev in rows
      r += %{ #{path}:#{rev}}
    end
    r += " ]\n"

    return r unless diff

    for path, rev, nrev in rows
      IO.popen('-') do |p|
	unless p
	  # child
	  path = File.join(@path, path) + ',v'
	  if not File.exists?(path)
	    pparts = File.split(path)
	    path = File.join(pparts[0], 'Attic', pparts[1])
	  end
	  # rcsdiff uses stderr to output the diff headers, so route it to stdout
	  $stderr.reopen($stdout)
	  exec 'rcsdiff', '-kb', "-r#{nrev}", "-r#{rev}", '-up', path
	else
	  # parent
	  r += p.readlines.join
	end
      end
    end

    return r
  end
end


opts = GetoptLong.new(
  ['--help', '-h', GetoptLong::NO_ARGUMENT],
  ['--build-incr', '-b', GetoptLong::NO_ARGUMENT],
  ['--build-new', '-B', GetoptLong::REQUIRED_ARGUMENT],
  ['--db', '-D', GetoptLong::REQUIRED_ARGUMENT],
  ['--nodiff', '-N', GetoptLong::NO_ARGUMENT]
)

dbfile = 'commits.db'
dobuild = false
rebuild = nil
diff = true
opts.each do |opt, arg|
  case opt
  when '--help'
    RDoc::usage
  when '--build-incr', '--build-new'
    dobuild = true
    rebuild = arg if opt == '--build-new'
  when '--db'
    dbfile = arg
  when '--nodiff'
    diff = false
  end
end

if dobuild
  cs = Commitset.new(dbfile, rebuild)
  cs.build(rebuild) do |state|
    puts "#{state}"
  end

  exit 0 if ARGV.length == 0
end

cs = Commitset.new(dbfile)

if ARGV.length != 2
  RDoc::usage(1)
end

puts cs.cset(*ARGV)
