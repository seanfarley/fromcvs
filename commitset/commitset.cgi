#!/usr/bin/env ruby

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

# == Synopsis
#
# commitset.cgi: provides a web interface to query CVS changesets from
#                a pre-built database
#
# == Usage
#
# Either put the commits.db in the directory you are running commitset.cgi
# from, or point the environment variable COMMITS_DB to this file.
#
# In Apache, you would put something like the following line into
# your .htaccess or the server config:
#
#     SetEnv COMMITS_DB /path/to/commits.db
#

require 'cgi'
require 'commitset'

cgi = CGI.new('html4')

if not cgi.key?('q')
  cgi.out do
    cgi.html do
      cgi.head{ cgi.title{'Commitset'} } +
      cgi.body do
	cgi.form('get') do
	  cgi.text_field('q') +
	  cgi.submit('Show changeset')
	end
      end
    end
  end
else
  cgi.print(cgi.header('text/plain'))

  begin
    cs = Commitset.new(ENV['COMMITS_DB'] || 'commits.db')
    cs.cset(*cgi['q'].split(' '))
  rescue StandardError => e
    cgi.print("An error occured: #{e}\n")
  end
end
