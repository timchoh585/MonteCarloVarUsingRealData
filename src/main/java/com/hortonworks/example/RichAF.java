package com.hortonworks.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class RichAF {

    private static JavaSparkContext sc = null;
    private static HiveContext sqlContext = null;


    public static void main(String[] args) throws Exception {
        Main m = new Main();
        m.run(args);
        m.close();
    }

    void close() {
        sc.stop();
    }

    Object run(String[] args) {
        /*
        Initializations
        */

        //Number of runs for program
        final int NUM_TRIALS = 1000000;

        //Specify the company list text
        String listOfCompanies = new File("stocks_list.txt").toURI().toString();
        //Specify the stock data directory
        String stockDataDir = "hdfs://sandbox.hortonworks.com/tmp/stockData/*.csv";

        if (sc == null) {
            //Create spark context
            SparkConf conf = new SparkConf().setAppName("monte-carlo-var-calculator");
            sc = new JavaSparkContext(conf);
            sqlContext = new HiveContext(sc);
        }

        /*
        * Read in total investment amount
        * Read in list of securities and randomly select two
        * Evenly Divide up total investment to randomly selected securities
        */
        //Entire stocks_list file
        JavaRDD<String> filteredFileRDD = sc.textFile(listOfCompanies).filter(s -> !s.startsWith("#") && !s.trim().isEmpty());
        //Obtain total money investable
        Long totalInvestement = sc.textFile(listOfCompanies).filter(s -> !s.startsWith("#") && !s.trim().isEmpty()).filter(s->s.startsWith("$")).longValue();
        //Number securities
        Long totalNumSecurities = filteredFileRDD.filter(s -> !s.startsWith("Portfilio")).filter(s->!s.startsWith("$")).count();
        //Randomly Select 2 stocks
        List<String> randomSecuritiesArray = filteredFileRDD.takeSample(false,2);
        //Convert list of randomly selected securities to RDD
        JavaRDD<String> randomSecuritiesRDD = sc.parallelize(randomSecuritiesArray);
        //Map each selected security into a pair with (Security,Money Allocated)
        JavaPairRDD<String, String> symbolsAndAmountRDD = randomlySecuritiesRDD.mapToPair(s ->
        {
            return new Tuple2<>(s, totalNumSecurities/2);
        });
        //Map the security to a dollar amount
        JavaPairRDD<String, Float> symbolsAndDollarsRDD = symbolsAndAmountRDD.mapToPair(x -> new Tuple2<>(x._1(), new Float(x._2()));

        /*
        * Read all stock trading data, and transform
        * Get a PairRDD of date -> (symbol, changeInPrice)
        * Reduce by key to get all dates together
        * Filter every date that doesn't have the max number of symbols
\       */

        //TODO Figure out how only get certain amount of dates ->look into JavaRDD.takeOrdered(int num, Comparator<T> comp)..idk how to implement it

        //Obtain (date,(symbol,changeInPrice)) pairRDD
        JavaPairRDD<String, Tuple2> datesToSymbolsAndChangeRDD = sc.textFile(stockDataDir).flatMapToPair(x -> {
            //skip header
            if (x.contains("Change_Pct")) {
                return Collections.EMPTY_LIST;
            }
            String[] splits = x.split(",", -2);
            Float changeInPrice = new Float(splits[7]);
            String symbol = splits[6];
            String date = splits[0];
            return Collections.singletonList(new Tuple2<>(date, new Tuple2<>(symbol, changeInPrice))); //(1/1/2017,(FB,.01)),(1/1/2017,(AAPL,.20))...
        });

        //Reduce by key to get all dates together
        JavaPairRDD<String, Iterable<Tuple2>> groupedDatesToSymbolsAndChangeRDD = datesToSymbolsAndChangeRDD.groupByKey();   //(Date,(Symbol,changeInPrice))

        //Filter every date that doesn't have the max number of symbols...Ensuring each stock is traded on the dates used
        long numSymbols = symbolsAndAmountRDD.count();
        Map<String, Object> countsByDate = datesToSymbolsAndChangeRDD.countByKey(); //Hashmap <K,int> of count of each key -> <Date, How many of that day>
        JavaPairRDD<String, Iterable<Tuple2>> filterdDatesToSymbolsAndChangeRDD = groupedDatesToSymbolsAndChangeRDD.filter(x -> (Long) countsByDate.get(x._1()) >= numSymbols); //Make sure there are are atleast trading data for each symbol on any given date
        long numEvents = filterdDatesToSymbolsAndChangeRDD.count(); //Number of dates -> still in this format: (Date,(Symbol,changeInPrice))

        //No usable trade data
        if (numEvents < 1) {
            System.out.println("No trade data");
            return "No trade data";
        }

        /* TODO
        * execute NUM_TRIALS?
        * Start at first possible date from the list of historical trade dates
        * Buy as much of each selected stock as possible
        * Update EquityValue of each stock each day
        */
        JavaPairRDD<String, Float> resultOfTrials = filterdDatesToSymbolsAndChangeRDD.mapToPair(i -> {
            Float total = 0f;
            Long portfolioReturn;

            for (Tuple2 t : i._2()) { //i = (Date,(Symbol,changeInPrice)) -> For every tuple (Symbol,changeInPrice) on given date, do basic calculations
                String symbol = t._1().toString();
                Float changeInPrice = new Float(t._2().toString());
                if(changeInPrice=0){
                  //Buy selected securities
                  //Each selected security has: avgPrice, numSharesBought, totalReturn, equityValue
                  //avgPrice=priceBoughtAt
                }
                else{
                  //equityValue = equityValue*changeInPrice
                  //totalReturn=(equityValue-(avgPrice*numSharesBought))/(avgPrice*numSharesBought)
                  //if(totalReturn>0.08 || totalReturn<-0.08)
                    //Sell()
                    //Buy()
                }
            }
            return new Tuple2<>(i._1(), total); //(Date,total % change in portfolio)
        });


        /*
        create a temporary table out of the data and take the 5%, 50%, and 95% percentiles

        1. multiple each float by 100
        2. create an RDD with Row types
        3. Create a schema
        4. Use that schema to create a data frame
        5. execute Hive percentile() SQL function
         */

        //I do not understand this part with the DB stuff
        JavaRDD<Row> resultOfTrialsRows = resultOfTrials.map(x -> RowFactory.create(x._1(), Math.round(x._2() * 100)));
        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("date", DataTypes.StringType, false), DataTypes.createStructField("changePct", DataTypes.IntegerType, false)});
        DataFrame resultOfTrialsDF = sqlContext.createDataFrame(resultOfTrialsRows, schema);
        resultOfTrialsDF.registerTempTable("results");
        List<Row> percentilesRow = sqlContext.sql("select percentile(changePct, array(0.05,0.50,0.95)) from results").collectAsList();

//        System.out.println(sqlContext.sql("select * from results order by changePct").collectAsList());
        float worstCase = new Float(percentilesRow.get(0).getList(0).get(0).toString()) / 100;
        float mostLikely = new Float(percentilesRow.get(0).getList(0).get(1).toString()) / 100;
        float bestCase = new Float(percentilesRow.get(0).getList(0).get(2).toString()) / 100;

        System.out.println("In a single day, this is what could happen to your stock holdings if you have $" + totalInvestement + " invested");
        System.out.println(String.format("%25s %7s %7s", "", "$", "%"));
        System.out.println(String.format("%25s %7d %7.2f%%", "worst case", Math.round(totalInvestement * worstCase / 100), worstCase));
        System.out.println(String.format("%25s %7d %7.2f%%", "most likely scenario", Math.round(totalInvestement * mostLikely / 100), mostLikely));
        System.out.println(String.format("%25s %7d %7.2f%%", "best case", Math.round(totalInvestement * bestCase / 100), bestCase));

        return worstCase;
    }
}
