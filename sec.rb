require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'mechanize'

$agent = Mechanize.new
$baseurl = "http://www.sec.gov"

# fetch the starting page...
$agent.get("#{$baseurl}/edgar/searchedgar/currentevents.htm")
form = $agent.page.form_with(:action=>"/cgi-bin/current.pl")
pp form
form['q1']="0"
form['q2']="3"
pp form
form.submit
puts $agent.page.parser.css('p strong')
