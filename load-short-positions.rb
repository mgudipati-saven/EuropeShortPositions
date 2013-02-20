=begin
  Redis DB layout for short positions data is defined as follows:
  
  key => "#{COUNTRY}:PositionHolders"
  val => sorted set of position holders for each country.
  
  key => "#{COUNTRY}:#{PositionHolder}:#{IssuerISIN}:Positions"
  val => hashtable of position date, net short position pairs for each position holder for each issuer.
  
=end

#!/usr/bin/env ruby -wKU
require 'csv'
require 'redis'
require 'getoptlong'

# call using "ruby load-short-positions.rb -i<input file>"  
unless ARGV.length == 1
  puts "Usage: ruby load-short-positions.rb -i<input file>" 
  exit  
end  
  
$infile = ''
# specify the options we accept and initialize the option parser  
opts = GetoptLong.new(  
  [ "--infile", "-i", GetoptLong::REQUIRED_ARGUMENT ]
)  

# process the parsed options  
opts.each do |opt, arg|  
  case opt  
    when '--infile'  
      $infile = arg  
  end  
end

# redis db connection
$redisdb = Redis.new
$redisdb.select 0

#
# Short positions file layout:
# POSITION HOLDER,NAME OF THE ISSUER,ISIN,NET SHORT POSITION,"DATE POSITION WAS CREATED, CHANGED OR CANCELLED"
# Clearance capital LLP,A&J Mucklow Group,GB0006091408,0.84%,2013-02-15
# Fox-Davies Capital Limited,African Medical Investments PLC,IM00B39HQT38,0.63%,2012-02-15
# ...
# ...
#
if $infile && File.exist?($infile)
  CSV.foreach($infile, :headers => true, :encoding => 'windows-1251:utf-8') do |row|
    # collect position holders in a sorted set - #{COUNTRY}:PositionHolders
    holder = row[0].strip
    if holder
      dbkey = "UNITED KINGDOM:PositionHolders"
      $redisdb.zadd dbkey, 0, holder

      # collect issuers in a Issuers hashtable
      issuer = row[1].strip
      isin = row[2].strip
      if issuer and isin
        dbkey = "Issuers"
        $redisdb.hset dbkey, isin, issuer
      end
      
      # short positions
      position = row[3].strip.chop # get rid of the %
      date = row[4].strip
      if position and date
        #key => "#{COUNTRY}:#{PositionHolder}:#{IssuerISIN}:Positions"
        dbkey = "UNITED KINGDOM:#{holder}:#{isin}:Positions"
        $redisdb.hset dbkey, date, position
      end
    end    
	end # CSV.foreach
end # if File.exist?($infile)

=begin rdoc
 * Name: load-short-positions.rb
 * Description: Loads short positions data from a csv file into redis db.
 * Call using "ruby load-short-positions.rb -i, --infile=<short positions file>"  
 * Author: Murthy Gudipati
 * Date: 20-Feb-2013
 * License: Saven Technologies Inc.
=end
