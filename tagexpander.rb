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

module FromCVS

class TagExpander
  def initialize(cvsroot)
    @cvsroot = cvsroot
    @keywords = {}
    expandkw = []
    self.methods.select{|m| m =~ /^expand_/}.each do |kw|
      kw[/^expand_/] = ''
      @keywords[kw] = kw
      expandkw << kw
    end

    configs = %w{config options}
    begin
      until configs.empty? do
        File.foreach(File.join(@cvsroot, 'CVSROOT', configs.shift)) do |line|
          if m = /^\s*(?:LocalKeyword|tag)=(\w+)(?:=(\w+))?/.match(line)
            @keywords[m[1]] = m[2] || 'Id'
          elsif m = /^\s*(?:KeywordExpand|tagexpand)=(e|i)(\w+(?:,\w+)*)?/.match(line)
            inc = m[1] == 'i'
            keyws = (m[2] || '').split(',')
            if inc
              expandkw = keyws
            else
              expandkw -= keyws
            end
          end
        end
      end
    rescue Errno::EACCES, Errno::ENOENT
      retry
    end

    if expandkw.empty?
      # produce unmatchable regexp
      @kwre = Regexp.compile('$nonmatch')
    else
      @kwre = Regexp.compile('\$('+expandkw.join('|')+')(?::.*?)?\$')
    end
  end

  def expand!(str, mode, rev)
    str.gsub!(@kwre) do |s|
      m = @kwre.match(s)    # gsub passes String, not MatchData
      case mode
      when 'o', 'b'
        s
      when 'k'
        "$#{m[1]}$"
      when 'kv', nil
        "$#{m[1]}: "  + send("expand_#{@keywords[m[1]]}", rev) + ' $'
      else
        s
      end
    end
  end

  def expand_Author(rev)
    rev.author
  end

  def expand_Date(rev)
    rev.date.strftime('%Y/%m/%d %H:%M:%S')
  end

  def _expand_header(rev)
    " #{rev.rev} " + expand_Date(rev) + " #{rev.author} #{rev.state}"
  end

  def expand_CVSHeader(rev)
    rev.rcsfile + _expand_header(rev)
  end

  def expand_Header(rev)
    File.join(@cvsroot, rev.rcsfile) + _expand_header(rev)
  end

  def expand_Id(rev)
    File.basename(rev.rcsfile) + _expand_header(rev)
  end

  def expand_Name(rev)
    if rev.syms
      rev.syms[0]
    else
      ""
    end
  end

  def expand_RCSfile(rev)
    File.basename(rev.rcsfile)
  end

  def expand_Revision(rev)
    rev.rev
  end

  def expand_Source(rev)
    File.join(@cvsroot, rev.rcsfile)
  end

  def expand_State(rev)
    rev.state.to_s
  end
end

end
