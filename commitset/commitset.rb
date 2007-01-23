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


class Commitset
  def initialize(dbfile)
    if not File.exists?(dbfile)
      raise Errno::ENOENT, dbfile
    end
    @db = SQLite3::Database.open(dbfile)
    @path = @db.get_first_value('SELECT value FROM meta WHERE key = "path"')
  end

  def _chsetstr(author, branch, date)
    %{Changeset by #{author}#{%{ on #{branch}} if branch} at #{date}}
  end

  def cset(file, rev, diff=true)
    file = file[0..-3] if file[-2..-1] == ',v'

    cset_id, branch, author, date = @db.get_first_row(%{
      SELECT * FROM cset WHERE cset_id = (
	SELECT cset_id FROM rev WHERE file_id = (
	  SELECT file_id FROM file WHERE path = :path
	) AND rev = :rev
      )
    }, ':path' => file, ':rev' => rev)
    raise RuntimeError, 'File or revision not found' unless cset_id
    date = Time.at(date.to_i)

    puts _chsetstr(author, branch, date)

    rcsf = File.join(@path, file) + ',v'
    begin
      RCSFile.open(rcsf) do |rf|
	puts rf.getlog(rev)
      end
    rescue Errno::ENOENT
      pc = File.split(rcsf)
      pc.insert(-2, 'Attic')
      rcsf = File.join(pc)
      retry
    end

    rows = @db.execute('SELECT path, rev, nrev FROM rev NATURAL JOIN file WHERE cset_id = ?',
		       cset_id)
    print '['
    for path, rev in rows
      print %{ #{path}:#{rev}}
    end
    puts ' ]'

    return unless diff

    for path, rev, nrev in rows
      path = File.join(@path, path) + ',v'
      if not File.exists?(path)
	pparts = File.split(path)
	path = File.join(pparts[0], 'Attic', pparts[1])
      end

      # rcsdiff can't output diffs for the first rev (1.1)
      # so we have to do so instead.
      if not nrev
	RCSFile.open(path) do |rf|
	  rl = rf.checkout(rev).split("\n")
	  puts <<END
===================================================================
RCS file: #{path}
diff -N #{path}
--- /dev/null\t#{Time.at(0)}
+++ #{File.basename(path)}\t#{date}\t#{rev}
@@ -0,0 +1,#{rl.length} @@
END
	  puts '+' + rl.join("\n+")
	end
	next
      end

      fork do
	# child
	# rcsdiff uses stderr to output the diff headers, so route it to stdout
	$stderr.reopen($stdout)
	exec 'rcsdiff', '-kb', "-r#{nrev}", "-r#{rev}", '-up', path
	raise StandardError, 'could not run rcsdiff'
      end
      Process.wait
    end

    nil
  end
end
