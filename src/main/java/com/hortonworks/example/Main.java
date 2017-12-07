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

public class Main {

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
        String listOfCompanies = new File("companies_list.txt").toURI().toString();
        //Specify the stock data directory
        String stockDataDir = "hdfs://sandbox.hortonworks.com/tmp/stockData/*.csv";

        if (args.length > 0) {
            listOfCompanies = args[0];
        }
        if (args.length > 1) {
            stockDataDir = args[1];
        }
        if (sc == null) {
            //Create spark context
            SparkConf conf = new SparkConf().setAppName("monte-carlo-var-calculator");
            sc = new JavaSparkContext(conf);
            sqlContext = new HiveContext(sc);
        }


        /*
        read a list of stock symbols and their weights in the portfolio, then transform into a Map<Symbol,Weight>
        1. read in the data, ignoring header
        2. convert dollar amounts to fractions
        3. create a local map
        */

        //So what this is doing is that this is taking the list of companies original format (TICKER, AMOUNT TO INVEST)
        //FB,$250
        //AAPL,$250
        //Makes a tuple out of the file
        //TODO We need it to randomly pick from this list and then invest an even or abritary % of our money into the stock


        JavaRDD<String> filteredFileRDD = sc.textFile(listOfCompanies).filter(s -> !s.startsWith("#") && !s.trim().isEmpty());
        JavaPairRDD<String, String> symbolsAndWeightsRDD = filteredFileRDD.filter(s -> !s.startsWith("Symbol")).mapToPair(s -> //Map each relevant line into a pair and store into symbolsAndWeights

        {
            String[] splits = s.split(",", -2);
            return new Tuple2<>(splits[0], splits[1]);
        });

        //convert from $ to % weight in portfolio
        Map<String, Float> symbolsAndWeights;
        Long totalInvestement;
        //Obtain second part of the tuple which is the dollar amount
        if (symbolsAndWeightsRDD.first()._2().contains("$")) { //Remove Dollar Sign from Tuple?? (FB, 250)(AAPL,250)
            JavaPairRDD<String, Float> symbolsAndDollarsRDD = symbolsAndWeightsRDD.mapToPair(x -> new Tuple2<>(x._1(), new Float(x._2().replaceAll("\\$", ""))));
            //Sum up all dollar values to obtain total amount of money invested (total,250+250) -> Access 2nd value in tuple as long -> (500)
            //Idk how it does this for each one though...
            totalInvestement = symbolsAndDollarsRDD.reduce((x, y) -> new Tuple2<>("total", x._2() + y._2()))._2().longValue();
            symbolsAndWeights = symbolsAndDollarsRDD.mapToPair(x -> new Tuple2<>(x._1(), x._2() / totalInvestement)).collectAsMap(); //Takes each tuple and gives % invested -> (FB, 0.50)(AAPL,0.50) as a local map

        } else {
            totalInvestement = 1000L; //Default??? idk?
            symbolsAndWeights = symbolsAndWeightsRDD.mapToPair(x -> new Tuple2<>(x._1(), new Float(x._2()))).collectAsMap();
        }

        /*
        read all stock trading data, and transform
        1. get a PairRDD of date -> (symbol, changeInPrice)
        2. reduce by key to get all dates together
        3. filter every date that doesn't have the max number of symbols
\        */

        //This is where it goes through the stock data

        // 1. get a PairRDD of date -> Tuple2(symbol, changeInPrice)
        JavaPairRDD<String, Tuple2> datesToSymbolsAndChangeRDD = sc.textFile(stockDataDir).flatMapToPair(x -> {
            //skip header
            if (x.contains("Change_Pct")) {
                return Collections.EMPTY_LIST;
            }
            String[] splits = x.split(",", -2); //Split the excel file by commas
            //There are 9 amount of header items originally -> we have [7] because 8 header items
            Float changeInPrice = new Float(splits[8]); //we have [7]
            String symbol = splits[7]; //we have [6]
            String date = splits[0]; //we have [0]
            return Collections.singletonList(new Tuple2<>(date, new Tuple2<>(symbol, changeInPrice))); //(1/1/2017,(FB,.01)),(1/1/2017,(AAPL,.20))...
        });

        //2. reduce by key to get all dates together
        JavaPairRDD<String, Iterable<Tuple2>> groupedDatesToSymbolsAndChangeRDD = datesToSymbolsAndChangeRDD.groupByKey();   //(Date,(Symbol,changeInPrice))


        //3. filter every date that doesn't have the max number of symbols
        long numSymbols = symbolsAndWeightsRDD.count(); //Number of symbols
        Map<String, Object> countsByDate = datesToSymbolsAndChangeRDD.countByKey(); //Hashmap <K,int> of count of each key -> <Date, How many of that day>
        JavaPairRDD<String, Iterable<Tuple2>> filterdDatesToSymbolsAndChangeRDD = groupedDatesToSymbolsAndChangeRDD.filter(x -> (Long) countsByDate.get(x._1()) >= numSymbols); //Make sure there are are atleast trading data for each symbol on any given date
        long numEvents = filterdDatesToSymbolsAndChangeRDD.count(); //Number of dates -> still in this format: (Date,(Symbol,changeInPrice))

        if (numEvents < 1) {
            System.out.println("No trade data");
            return "No trade data";
        }

        /*
        execute NUM_TRIALS
        1. pick a random date from the list of historical trade dates
        2. sum(stock weight in overall portfolio * change in price on that date)
         */
        double fraction = 1.0 * NUM_TRIALS / numEvents; //100/20 = 5.0

        JavaPairRDD<String, Float> resultOfTrials = filterdDatesToSymbolsAndChangeRDD.sample(true, fraction).mapToPair(i -> { //Sample fraction of the data with replacement for each date
            Float total = 0f;

            for (Tuple2 t : i._2()) { //i = (Date,(Symbol,changeInPrice)) -> For every tuple (Symbol,changeInPrice) on given date, do basic calculations
                String symbol = t._1().toString();
                Float changeInPrice = new Float(t._2().toString());
                Float weight = symbolsAndWeights.get(symbol);

                total += changeInPrice * weight;
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
