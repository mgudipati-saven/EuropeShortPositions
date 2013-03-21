=begin
  Scrape Germany short positions using advanced search method. This will fetch all positions ever held.

    page.css('div.content table.result tr td a')[0..-1]['href']
    page.css('div.content table.result.nlp_history tr')[1..-1].css('td')[3].text
  
  Navigation:
    page.css('div.content li.pagelinks ul.page_navigation li.next_page a')[1]['href']
=end

require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'mechanize'

$agent = Mechanize.new

$db = SQLite3::Database.new('shorts.sqlite')
$db.execute "CREATE TABLE IF NOT EXISTS ShortPositions(
  holder    VARCHAR(64) NOT NULL,
  issuer    VARCHAR(64) NOT NULL,
  isin      VARCHAR(16) NOT NULL,
  position  FLOAT NOT NULL,
  date      DATE NOT NULL)"

$baseurl = "https://www.bundesanzeiger.de"

#
# Function to scrape the positions table given the page...
#
def scrape(page)
  rows = page.css('div.content table.result tbody tr')[0..-1]
  rows.each do |row|
    puts row.text.strip    
    $db.execute "INSERT INTO ShortPositions 
      (holder, issuer, isin, position, date) 
      VALUES 
      (?,?,?,?,?)",
      [
        row.css('td')[0].text.strip,
        row.css('td')[1].children[0].text.strip,
        row.css('td')[2].text.strip,
        row.css('td')[3].text.strip.chop.strip.gsub(',','.').to_f,
        row.css('td')[4].text.strip
      ]
  end
end

#
# Recursive function to go through each page and scrape out positions table
#
def navigate(page)
  puts page.css('div.content li.first.entry_count b')
  # scrape the table...
  scrape(page)
  
  # check for pagination...
  links = page.css('div.content li.pagelinks ul.page_navigation li.next_page a')
  if !links.empty?
    relurl = links[0]['href']
    puts relurl
    # recursion, go to next page and scrape...
    $agent.get("#{$baseurl}#{relurl}")
    navigate($agent.page.parser)
  end
end

# fetch the starting page...
$agent.get("#{$baseurl}/ebanzwww/wexsservlet?page.navid=to_nlp_start")

# fetch the advanced search url...
link = $agent.page.parser.css('div.nlp_top_search.clearfix a.intern')[0]['href']
$agent.get("#{$baseurl}#{link}")
puts $agent.page.parser.css('div.content div.error_text')

# fetch the advanced search form...
form = $agent.page.form_with(:action=>/ebanzwww\/wexsservlet/)
if form
  # set start date...
  form['nlp_search_param.date_start:0']="1"
  form['nlp_search_param.date_start:1']="11"
  form['nlp_search_param.date_start:2']="2012"

  # set end date...
  form['nlp_search_param.date_end:0']="21"
  form['nlp_search_param.date_end:1']="3"
  form['nlp_search_param.date_end:2']="2013"

  # set history flag...
  form['nlp_search_param.search_history']="true"

  # submit the advanced search form...
  form.submit(form.button_with(:value=>/suchen/))

  # process the new page...
  navigate($agent.page.parser)
end
