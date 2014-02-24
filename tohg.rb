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
require 'fileutils'

# THE FOLLOWING FRAGMENT WAS COPIED FROM THE RUBY SOURCE
# IT IS UNDER THE SAME COPYRIGHT AS THE ORIGINAL, open3.rb.
#
# However, I modified it to just rewire stdin and stdout.
module Open2
  #[stdin, stdout] = popen2(command);
  def popen2(*cmd)
    pw = IO::pipe   # pipe[0] for read, pipe[1] for write
    pr = IO::pipe

    pid = fork{
      # child
      fork{
	# grandchild
	pw[1].close
	STDIN.reopen(pw[0])
	pw[0].close

	pr[0].close
	STDOUT.reopen(pr[1])
	pr[1].close

	exec(*cmd)
      }
      exit!(0)
    }

    pw[0].close
    pr[1].close
    Process.waitpid(pid)
    pi = [pw[1], pr[0]]
    pw[1].sync = true
    if defined? yield
      begin
	return yield(*pi)
      ensure
	pi.each{|p| p.close unless p.closed?}
      end
    end
    pi
  end
  module_function :popen2
end
# END COPY

module FromCVS

class HGDestRepo
  attr_reader :revs_with_cset
  attr_reader :revs_per_file
  attr_reader :last_date

  def initialize(hgroot, status=lambda{|s|})
    @revs_per_file = false
    @revs_with_cset = true
    @hgroot = hgroot
    @status = status

    @outs, @ins = \
      Open2.popen2('python', File.join(File.dirname($0), 'tohg.py'), hgroot)
    @last_date = Time.at(@ins.readline.strip.to_i)
    @branches = {}
    while l = @ins.readline do
      l.strip!
      break if l.empty?
      br, n = l.split
      @branches[br] = n
    end
  end

  def filelist(tag)
    node = @branches[tag || 'HEAD']
    if tag == :complete
      return @branches.map{|b, _| filelist(b)}.flatten.uniq
    end
    if not node
      return []
    end
    @outs.puts("filelist #{node}")
    files = []
    while l = @ins.readline("\0") do
      break if l == "\0"
      files << l[0..-2]
    end
    @ins.readline   # eat newline
    files
  end

  def start
    @commits = 0
    @files = []
  end

  def flush(force=false)
    return if @commits < 10 and not force
    @commits = 0
    @outs.puts 'flush'
  end

  def has_branch?(branch)
    @branches.include?(branch || 'HEAD')
  end

  def branch_id(branch)
    @branches[branch || 'HEAD']
  end

  def create_branch(branch, parent, vendor_p, date)
    return if @branches.include? branch

    if vendor_p
      node = '0'*40
      text = "creating vendor branch #{branch}"
    else
      parent ||= 'HEAD'
      node = @branches[parent]
      status "creating branch #{branch} from #{parent}, cset #{node.unpack('H12')}"
      text = "creating branch #{branch} from #{parent}"
    end

    newnode = _commit("repo convert", date, text, [], nil, node, branch)
    @branches[branch] = newnode    # mercurial does not keep track itself
    newnode
  end

  def select_branch(branch)
    @curbranch = branch || 'HEAD'
  end

  def remove(file, rev)
    begin
      repof = File.join(@hgroot, file)
      File.unlink(repof)
    rescue Errno::ENOENT
      # well, it is gone already
    end
    @files << file
  end

  def update(file, data, mode, uid, gid, rev)
    mode |= 0666
    mode |= 0111 if mode & 0111 != 0
    repof = File.join(@hgroot, file)
    FileUtils.makedirs(File.dirname(repof))
    File.open(repof, File::CREAT|File::TRUNC|File::RDWR, mode) do |f|
      f.write(data)
    end
    @files << file
  end

  def commit(author, date, msg, revs)
    status "committing set by #{author} at #{date} to #@curbranch"
    node = _commit(author, date, msg, @files)
    if not node
      raise RuntimeError, "could not commit set (%s)" %
            revs.collect{|r| "#{r.file}:#{r.rev}"}.join(' ')
    end
    @files = []
    node
  end

  def merge(branch, author, date, msg, revs)
    status "merging cset #{branch.unpack('H12')[0]} by #{author} at #{date} to #@curbranch"
    node = _commit(author, date, msg, @files, branch)
    @files = []
    node
  end

  def finish
    @outs.puts 'finish'
  end

  private
  def _commit(author, date, msg, files, p2=nil, p1=nil, branch=nil)
    # per default commit to @curbranch
    p1 ||= @branches[@curbranch]
    branch ||= @curbranch

    # if can happen that HEAD has not been created yet
    if not p1
      if @curbranch != "HEAD"
        raise StandardError, "branch #{@curbranch} not yet created"
      end
      p1 = '0'*40
    end

    @outs.puts 'commit'
    @outs.puts author
    @outs.puts date.to_i.to_s
    @outs.puts "#{p1}"
    @outs.puts "#{p2}"
    @outs.puts branch
    files.each do |f|
      @outs.write("#{f}\0")
    end
    @outs.puts "\0"
    @outs.puts "#{msg.bytesize}\n#{msg}\n"

    node = @ins.readline.strip
    @commits += 1
    @branches[branch] = node   # not kept up-to-date by mercurial :/
    node
  end

  def status(str)
    @status.call(str) if @status
  end
end


if $0 == __FILE__
  status = lambda do |str|
    puts str
  end

  params = Repo.parseopt([]) {}

  if ARGV.length != 3
    puts "call: tohg <cvsroot> <module> <hgdir>"
    exit 1
  end

  cvsdir, modul, hgdir = ARGV

  hgrepo = HGDestRepo.new(hgdir, status)
  cvsrepo = Repo.new(cvsdir, hgrepo, params)
  cvsrepo.status = status
  cvsrepo.scan(modul)
  cvsrepo.commit_sets
end

end   # module FromCVS
