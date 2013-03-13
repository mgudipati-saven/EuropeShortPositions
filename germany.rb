=begin
  Scrape Germany Short Positions

  Positions table:
    page.css('div.content table.result tr td a')[0..-1]['href']
    page.css('div.content table.result.nlp_history tr')[1..-1].css('td')[3].text
  
  Navigation:
    page.css('div.content li.pagelinks ul.page_navigation li.next_page a')[1]['href']
=end

require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'sqlite3'

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
def scrape(page, baseurl)
  rows = page.css('div.content table.result tr td a')[0..-1]
  puts "Number of rows: #{rows.length}"
  rows.each do |row|
    puts row.text.strip
    # fetch the history page for this issuer...
    relurl = row['href']
    url = baseurl+relurl
    puts "*******************************History table => #{url}"
    page = Nokogiri::HTML(open(url))

    # scrape the history positions table...
    recs = page.css('div.content table.result.nlp_history tr')[1..-1]
    puts "Number of recs: #{recs.length}"
    recs.each do |rec|
      puts rec.text.strip
      $db.execute "INSERT INTO ShortPositions 
        (holder, issuer, isin, position, date) 
        VALUES 
        (?,?,?,?,?)",
        [
          rec.css('td')[0].text.strip,
          rec.css('td')[1].text.strip,
          rec.css('td')[2].text.strip,
          rec.css('td')[3].text.strip.chop.strip.gsub(',','.').to_f,
          rec.css('td')[4].text.strip
        ]
    end
  end
end

#
# Recursive function to go through each page and scrape out positions table
#
def navigate(baseurl, relurl)
  # fetch the positions table page...
  url = baseurl+relurl
  puts "============================ Positions table => #{url}"
  page = Nokogiri::HTML(open(url))

  # scrape the table...
  scrape(page, baseurl)
  
  # check for pagination...
  links = page.css('div.content li.pagelinks ul.page_navigation li.next_page a')
  if !links.empty?
    relurl = links[0]['href']

    # recursion, go to next page and scrape...
    navigate(baseurl, relurl)
  end
end

# navigate through each page and scrape table data...
relurl = "/ebanzwww/wexsservlet?page.navid=to_nlp_start"
navigate($baseurl, relurl)
