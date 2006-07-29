# == Synopsis
#
# commitset: outputs a CVS changeset from a pre-built database
#
# == Usage
#
# commitset [-hN] [-b path] [-D dbfile] file rev
#
# -b path, --build path:
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
    attr_accessor :id, :author, :date

    def update_id!
      @id = MD5.md5
      self.each do |r|
	@id << r.file << r.rev
      end
    end

    def <<(rev)
      super

      if not @branches
	@branches = rev.branches.dup if not rev.branches.empty?
      else
	@branches &= rev.branches
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
    for r in revs
      if not set.empty? and not set[-1].same_set?(r)
	@sets << set
	set = Set.new
      end
      set << r
    end
    @sets << set

    self
  end
end


class Commitset
  def initialize(dbfile, create=false)
    if not create and not File.exists?(dbfile)
      raise Errno::ENOENT, dbfile
    end
    @db = SQLite3::Database.open(dbfile)
    _init_schema
    @path = @db.get_first_value('SELECT path FROM meta LIMIT 1')
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
	path TEXT NOT NULL
      );

      -- make SQLite3 happy, it checks something it shouldn't
      -- so give it a result to chew on.
      SELECT 1;
    })
  end

  def build(path)
    @db.execute_batch(%{
      DROP TABLE cset;
      DROP TABLE file;
      DROP INDEX rev_cset;
      DROP TABLE rev;
      DROP TABLE meta;

      SELECT 1;
    })
    _init_schema
    @db.execute('INSERT INTO meta VALUES ( ? )', path)
    @path = path

    r = Repo.new(@path)
    r.scan!

    n = 0
    for s in r.sets
      n += 1

      @db.transaction do |tdb|
	if block_given?
	  yield s, n, r.sets.length
	end

	tdb.execute('INSERT INTO cset VALUES ( NULL, :branch, :author, :date )',
		    ':branch' => s.branch,
		    ':author' => s.author,
		    ':date' => s.date.to_i
		   )
	cset_id = tdb.last_insert_row_id
	for rev in s
	  fid = tdb.get_first_value('SELECT file_id FROM file WHERE path = ?', rev.file)
	  unless fid
	    tdb.execute('INSERT INTO file VALUES ( NULL, ? )', rev.file)
	    fid = tdb.last_insert_row_id
	  end
	  tdb.execute('INSERT INTO rev VALUES ( :fid, :rev, :nrev, :cset_id )',
		      ':fid' => fid,
		      ':rev' => rev.rev,
		      ':nrev' => rev.next,
		      ':cset_id' => cset_id
		     )
	end
      end
    end
  end

  def cset(file, rev, diff=true)
    r = ""

    cset_id, branch, author, date = @db.get_first_row(%{
      SELECT * FROM cset WHERE cset_id = (
	SELECT cset_id FROM rev WHERE file_id = (
	  SELECT file_id FROM file WHERE path = :path
	) AND rev = :rev
      )
    })
    date = Time.at(date.to_i)

    r += %{Changeset by #{author} #{%{ on #{branch}} if branch} at #{date}\n}

    log = nil
    rows = @db.execute('SELECT path, rev, nrev FROM rev JOIN file WHERE cset_id = ?',
		       cset_id)
    raise RuntimeError, 'File or revision not found' unless rows

    RCSFile.open(File.join(@path, rows[0][0])) do |rf|
      r += rf.getlog(rev) + "\n"
    end

    r += '['
    for path, rev in rows
      r += %{ #{path} #{rev}}
    end
    r += " ]\n"
  end
end


opts = GetoptLong.new(
  ['--help', '-h', GetoptLong::NO_ARGUMENT],
  ['--build', '-b', GetoptLong::REQUIRED_ARGUMENT],
  ['--db', '-D', GetoptLong::REQUIRED_ARGUMENT],
  ['--nodiff', '-N', GetoptLong::NO_ARGUMENT]
)

dbfile = 'commits.db'
dobuild = nil
diff = true
opts.each do |opt, arg|
  case opt
  when '--help'
    RDoc::usage
  when '--build'
    dobuild = arg
  when '--db'
    dbfile = arg
  when '--nodiff'
    diff = false
  end
end

if dobuild
  cs = Commitset.new(dbfile, true)
  cs.build(dobuild) do |s, n, tot|
    puts %{Commitset #{n}/#{tot} by #{s.author} at #{s.date}}
  end

  exit 0 if ARGV.length == 0
end

cs = Commitset.new(dbfile)

if ARGV.length != 2
  RDoc::usage(1)
end

puts cs.cset(*ARGV)
