#!/usr/bin/env ruby -wKU
require 'csv'
require 'redis'

# redis db connection
$redisdb = Redis.new
$redisdb.select 0

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
  "ISSUER,ISIN",
  "NET SHORT POSITION (%)",
  "POSITION DATE"
  ]

CSV.open("historical-short-positions.csv", "wb", :headers => headers_a, :write_headers => true) do |csv|
  # get all the position holders for each country
  holders_a = $redisdb.zrange "UNITED KINGDOM:PositionHolders", 0, -1
  holders_a.each do |holder|
    isins_a = $redisdb.hkeys "Issuers"
    isins_a.each do |isin|
      # check if the holder holds a position in an issuer
      dbkey = "UNITED KINGDOM:#{holder}:#{isin}:Positions"
      if $redisdb.exists dbkey
        date_a = $redisdb.hkeys dbkey
        # obtain the latest date for which the holder holds a position
        #date = date_a.sort.last

        # obtain the net short positions for all the dates sorted
        date_a.sort.each do |date|
          pos = $redisdb.hget dbkey, date
          if pos
            issuer = $redisdb.hget "Issuers", isin
            csv << ["UNITED KINGDOM", holder, issuer, isin, pos, date]
          end
        end
      end
    end
  end
end

