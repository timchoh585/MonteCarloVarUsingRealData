#!/bin/bash
#
# Downloads previous twenty days of trading data from AlphaVantage API
# Stores output into CSV format and calculates % Change in Daily Pricing
#

#Setup
destinationDir=${1:-/tmp/stockData/}
echo Running as $(id -un) on $(uname -n):$(pwd -P)
echo Will save data to $destinationDir
t=$(mktemp -d /tmp/downloadStock-input-XXXXX)
i=0
#Obtain list of companies
oldestCompanies=$(cat companies_list.txt |  egrep -v '^(#.*|\s+)$' | tail -n +2 | cut -d',' -f1)
for s in $oldestCompanies; do
  #Columns of API data: Date,Open,High,Low,Close,Volume
  url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=$s&apikey=T6SPHY851Z5F4UWO&datatype=csv"
  echo Downloading historical data for $s
  (
  #Curl API Call
  curl -s "$url" | tail -n +2  | tac > $t/${s}2.csv
  #Eliminate carriage return for unix
  tr '\r' '\n' < $t/${s}2.csv > $t/${s}.csv
  rm $t/${s}2.csv
  #Add Symbol and Change % columns to csv
  lastPrice= echo "Date,Open,High,Low,Close,Volume,Symbol,Change_Pct" > $t/${s}_2.csv
  #Loop through each row
  for l in $(cat $t/${s}.csv  ); do
    currentPrice=$(echo $l | rev | cut -d',' -f 2 | rev)
    changeInPrice=0
    if [ -n "$lastPrice" ]; then
    changeInPrice=$(bc -l <<< "scale=4;($currentPrice/$lastPrice -1)*100")
    fi
    lastPrice=$currentPrice
    echo "$l,$s,$changeInPrice" >> $t/${s}_2.csv
  done
  #Clean Up
  rm -f "$t/${s}.csv"
  mv $t/${s}_2.csv $t/${s}.csv
  ) &
  i=$(( $i + 1 ))
  if [ $i -ge 20 ]; then
  i=0
  wait
  fi
done
wait
#Run Spark?
touch $t/_SUCCESS
hdfs dfs -rm -R -skipTrash "$destinationDir"
hdfs dfs -mkdir -p "$destinationDir" && hdfs dfs -put -f $t/* "$destinationDir"; hdfs dfs -put -f companies_list.txt "$destinationDir"
rm -fr "$t"
echo Saved to $destinationDir
