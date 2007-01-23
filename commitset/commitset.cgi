#!/usr/bin/env ruby

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
