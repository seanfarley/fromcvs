require 'rcsfile'
require 'find'
require 'md5'
require 'sqlite3'

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


class Commitset
  def initialize(dbfile)
    @db = SQLite3::Database.open('commitsets.db')
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
	name TEXT NOT NULL UNIQUE
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

    for s in r.sets
      @db.transaction do |tdb|
	puts "changeset by #{s.author} at #{s.date}" + \
	  if s.branch
	    " on #{s.branch}"
	  else
	    ""
	  end

	tdb.execute('INSERT INTO cset VALUES ( NULL, :branch, :author, :date )',
		    ':branch' => s.branch,
		    ':author' => s.author,
		    ':date' => s.date.to_i
		   )
	cset_id = tdb.last_insert_row_id
	for rev in s
	  fid = tdb.get_first_value('SELECT file_id FROM file WHERE name=?', rev.file)
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
end


cs = Commitset.new('commitsets.db')
cs.build('/space/cvs/dragonfly/src/sys')
