=begin
  Redis DB layout for short positions data is defined as follows:

  key => "Countries"
  val => sorted set of countries.

  key => "Issuers"
  val => hashtable of issuer isin => name mapping.
  
  key => "#{COUNTRY}:PositionHolders"
  val => sorted set of position holders.
  
  key => "#{COUNTRY}:#{PositionHolder}:#{IssuerISIN}:Positions"
  val => hashtable of position date, net short position pairs.
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
$redisdb.select 1

#
# Short positions file layout:
# COUNTRY,POSITION HOLDER,NAME OF THE ISSUER,ISIN,NET SHORT POSITION,"DATE POSITION WAS CREATED, CHANGED OR CANCELLED"
# UNITED KINGDOM,Clearance capital LLP,A&J Mucklow Group,GB0006091408,0.84%,2013-02-15
# UNITED KINGDOM,Fox-Davies Capital Limited,African Medical Investments PLC,IM00B39HQT38,0.63%,2012-02-15
# ...
# ...
#
if $infile && File.exist?($infile)
  CSV.foreach($infile, :headers => true, :encoding => 'windows-1251:utf-8') do |row|
    if row[0]
      # collect countries in a sorted set - Countries
      country = row[0].upcase.strip
      $redisdb.zadd :Countries, 0, country

      if row[1]
        # collect position holders in a sorted set - #{COUNTRY}:PositionHolders
        holder = row[1].upcase.strip
        $redisdb.zadd "#{country}:PositionHolders", 0, holder

        # collect issuers in a Issuers hashtable - isin => name
        if row[2] and row[3]
          issuer = row[2].upcase.strip
          isin = row[3].strip
          $redisdb.hset :Issuers, isin, issuer
        end

        # short positions
        if row[4] and row[5]
          position = row[4].strip.chop # get rid of the %
          date = row[5].strip
          $redisdb.hset "#{country}:#{holder}:#{isin}:Positions", date, position
        end
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
