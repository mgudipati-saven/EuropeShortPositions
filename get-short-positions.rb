#!/usr/bin/env ruby -wKU
require 'csv'
require 'redis'
require 'getoptlong'

# call using "ruby load-short-positions.rb -o<output file> [-m<mode>]"
unless ARGV.length >= 1
  puts "Usage: ruby get-short-positions.rb -o<output file> [-m<mode>]" 
  exit  
end  
  
$outfile = ''
$mode = 'curr'
# specify the options we accept and initialize the option parser  
opts = GetoptLong.new(  
  [ "--outfile", "-o", GetoptLong::REQUIRED_ARGUMENT ],
  [ "--mode", "-m", GetoptLong::OPTIONAL_ARGUMENT ]
)  

# process the parsed options  
opts.each do |opt, arg|  
  case opt  
    when '--outfile'  
      $outfile = arg  

    when '--mode'
      if arg == 'curr' or arg == 'hist'
        $mode = arg
      end
  end  
end

# redis db connection
$redisdb = Redis.new
$redisdb.select 1

#
# Short positions file layout:
# COUNTRY,HOLDER NAME,ISSUER,ISIN,NET SHORT POSITION (%),POSITION DATE
# UNITED KINGDOM,Clearance capital LLP,A&J Mucklow Group,GB0006091408,0.84,2013-02-15
# UNITED KINGDOM,Fox-Davies Capital Limited,African Medical Investments PLC,IM00B39HQT38,0.63,2012-02-15
# ...
# ...
#
headers_a = [
  "COUNTRY",
  "HOLDER NAME",
  "ISSUER",
  "ISIN",
  "NET SHORT POSITION (%)",
  "POSITION DATE"
  ]

CSV.open($outfile, "wb", :headers => headers_a, :write_headers => true) do |csv|
  # for all countries...
  countries_a = $redisdb.zrange :Countries, 0, -1
  countries_a.each do |country|
    # get all the position holders for each country
    holders_a = $redisdb.zrange "#{country}:PositionHolders", 0, -1
    holders_a.each do |holder|
      isins_a = $redisdb.hkeys :Issuers
      isins_a.each do |isin|
        # check if the holder holds a position in an issuer
        dbkey = "#{country}:#{holder}:#{isin}:Positions"
        if $redisdb.exists dbkey
          date_a = $redisdb.hkeys dbkey
          if $mode == 'curr'
            # obtain the latest date for which the holder holds a position
            date = date_a.sort.last
            pos = $redisdb.hget dbkey, date
            if pos
              issuer = $redisdb.hget :Issuers, isin
              csv << [country, holder, issuer, isin, pos, date]
            end
          else
            # obtain the net short positions for all the dates sorted
            date_a.sort.each do |date|
              pos = $redisdb.hget dbkey, date
              if pos
                issuer = $redisdb.hget :Issuers, isin
                csv << [country, holder, issuer, isin, pos, date]
              end
            end
          end
        end
      end
    end
  end
end