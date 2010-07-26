# This file is part of fromcvs.
#
# fromcvs is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# fromcvs is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with fromcvs.  If not, see <http://www.gnu.org/licenses/>.
#

require 'fromcvs'
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


module FromCVS

class DbDestRepo
  class << self
    private :new

    def open(dbfile, status=lambda{|s|})
      new(dbfile, false, nil, nil, status)
    end

    def create(dbfile, cvsroot, modules, status=lambda{|s|})
      new(dbfile, true, cvsroot, modules, status)
    end
  end

  attr_reader :path
  attr_reader :revs_with_cset
  attr_reader :revs_per_file

  def initialize(dbfile, create, path, modules, status=lambda{|s|})
    @revs_per_file = false
    @revs_with_cset = false

    if not create and not File.exists?(dbfile)
      raise Errno::ENOENT, dbfile
    end

    @db = SQLite3::Database.open(dbfile)
    if create
      @path = path
      _init_schema(modules)
    else
      @path = @db.get_first_value('SELECT value FROM meta WHERE key = "path"')
    end
    @db.execute('PRAGMA synchronous = OFF')
  end

  def _init_schema(modules)
    @db.execute_batch(%{
      DROP TABLE IF EXISTS cset;
      DROP TABLE IF EXISTS file;
      DROP INDEX IF EXISTS rev_cset;
      DROP TABLE IF EXISTS rev;
      DROP TABLE IF EXISTS meta;

      SELECT 1;
    })
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
    @db.execute('INSERT INTO meta VALUES ( "path", ? )', @path)
    @db.execute('INSERT INTO meta VALUES ( "modules", ? )', modules.join(' '))
  end
  private :_init_schema

  def last_date
    begin
      date = @db.get_first_value('SELECT date, cset_id FROM cset 
                                  ORDER BY cset_id DESC LIMIT 1')
    rescue SQLite3::SQLException
      date = 0
    end

    Time.at(date.to_i)
  end

  def module_list
    @db.get_first_value('SELECT value FROM meta WHERE key = "modules"').split
  end

  def filelist(tag)
    []
  end

  def start
    @db.transaction
  end

  def flush
    @db.commit
    @db.transaction
  end

  def has_branch?(branch)
  end

  def branch_id(branch)
  end

  def create_branch(branch, parent, vendor_p, date)
  end

  def select_branch(branch)
    @curbranch = branch 
  end

  def remove(file, rev)
  end

  def update(file, data, mode, uid, gid, rev)
  end

  def commit(author, date, msg, revs)
    # we don't want a real repo with branches
    return if author == 'branch-fixup'

    @db.execute('INSERT INTO cset VALUES ( NULL, :branch, :author, :date )',
                ':branch' => @curbranch,
                ':author' => author,
                ':date' => date.to_i
               )
    cset_id = @db.last_insert_row_id

    revs.each do |rev|
      fid = @db.get_first_value('SELECT file_id FROM file WHERE path = ?', rev.file)
      unless fid
        @db.execute('INSERT INTO file VALUES ( NULL, ? )', rev.file)
        fid = @db.last_insert_row_id
      end
      @db.execute('INSERT INTO rev VALUES ( :fid, :rev, :nrev, :cset_id )',
                  ':fid' => fid,
                  ':rev' => rev.rev,
                  ':nrev' => (rev.link || rev.next),
                  ':cset_id' => cset_id
                 )
    end
  end

  def merge(branch, author, date, msg, revs)
    # ignore merges, they are either branch->trunk
    # or forward merges to child branches.
    # in either case, there are no new revs involved.
  end

  def finish
    @db.commit
  end
end


if $0 == __FILE__
  status = lambda do |str|
    $stderr.puts str
  end

  params = Repo.parseopt([]) {}

  if ARGV.length != 3
    puts "call: todb <cvsroot> <module> <dbfile>"
    exit 1
  end

  cvsdir, modul, dbfile = ARGV

  dbrepo = DbDestRepo.create(dbfile, cvsdir, [modul], status)
  cvsrepo = Repo.new(cvsdir, dbrepo, params)
  cvsrepo.status = status
  cvsrepo.scan(modul)
  cvsrepo.commit_sets
end

end     # module FromCVS
