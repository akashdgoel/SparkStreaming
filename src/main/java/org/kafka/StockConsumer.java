package org.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParser;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.ini4j.Wini;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import static java.lang.Math.abs;

public class StockConsumer implements Serializable {
    static String boostrapServer;
    static String topicName;
    static String groupId;
    JavaStreamingContext jsc;
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws InterruptedException {
        boostrapServer = args[0];
        topicName = args[1];
        groupId = args[2];
        StockConsumer obj = new StockConsumer();
        readStockData();
    }

    public static void readStockData() throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Kafka_SparkStreaming")
                .set("spark.streaming.backpressure.enabled", "true")
                .setMaster("local[1]");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(60));//Setting batch interval to 1 minute
        jsc.sparkContext().setLogLevel("WARN");
        jsc.checkpoint("checkpoint_dir");//Setting checkpoint Directory
        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("bootstrap.servers", boostrapServer);
                //put("auto.offset.reset", "smallest");
                put("group.id", groupId);
            }
        };

        JavaPairInputDStream<String, String> messages =
                KafkaUtils.createDirectStream(jsc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        Collections.singleton(topicName));

        JavaDStream<String> records = messages.map(t -> t._2());
        records.print();

        //Problem 1 - Simple moving average closing price of the four stocks in a 5-minute sliding window for the last 10 minutes
        readClosingData(records);

        //Problem 2 - stock out of the four stocks giving maximum profit in a 5-minute sliding window for the last 10 minutes
        readProfitData(records);

        //Problem 3 - Trading volume(total traded volume) of the four stocks every 10 minutes and decide which stock to purchase out of the four stocks
        readVolumeData(records);

        jsc.start();
        jsc.awaitTermination();
    }


    public static void readClosingData(JavaDStream<String> records) {
        //Generate a pair stream in form of key value pairs for symbol and closing price
        JavaPairDStream<String, Double> closingData = records.mapToPair(new PairFunction<String, String, Double>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Double> call(String x) throws Exception {

                //creating mapper object
                ObjectMapper mapper = new ObjectMapper();

                // defining the return type
                TypeReference<Stock> mapType = new TypeReference<Stock>() {
                };

                // Parsing the JSON String
                Stock stock = mapper.readValue(x, mapType);

                return new Tuple2<String, Double>(stock.getSymbol(), stock.getPriceData().getClose());

            }
        });

        //Reduce function for summation of closing amount
        Function2<Double, Double, Double> reduceSumFunc = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double pricePresent, Double priceIncoming) throws Exception {
                System.out.println("Reduce Sum function is running");
                //System.out.println(pricePresent + "+" + priceIncoming);

                return (pricePresent + priceIncoming);
            }
        };

        //Inverse function for subtracting closing amount from sliding window
        Function2<Double, Double, Double> invSumFunc = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double pricePresent, Double priceOutgoing) throws Exception {
                System.out.println("Inverse Sum function is running");
                //System.out.println("PriceOutgoing= " + priceOutgoing);
                //System.out.println(pricePresent + "-" + priceOutgoing);
                return (pricePresent - priceOutgoing);
            }
        };

        //Reduce function for average of closing amount
        Function2<Double, Double, Double> reduceAvgFunc = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double pricePresent, Double priceIncoming) throws Exception {
                System.out.println("Reduce Avg function is running");
                System.out.println("Reduce Avg=" + priceIncoming + "/10");

                return (priceIncoming) / 10;
            }
        };

        //Inverse function for subtracting average of closing amount from the sliding window
        Function2<Double, Double, Double> invAvgFunc = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double pricePresent, Double priceOutgoing) throws Exception {
                System.out.println("Inverse Avg function is running");
                //System.out.println("PriceOutgoing= " + priceOutgoing);
                //System.out.println(pricePresent + "-" + "(" + priceOutgoing + ")/10");
                return (pricePresent - (priceOutgoing) / 10);
            }
        };

        JavaPairDStream<String, Double> sumByKey = closingData.
                reduceByKeyAndWindow(reduceSumFunc, invSumFunc, Durations.seconds(600), Durations.seconds(300));

        JavaPairDStream<String, Double> avgByKey = sumByKey.
                reduceByKeyAndWindow(reduceAvgFunc, invAvgFunc, Durations.seconds(600), Durations.seconds(300));

        avgByKey.print();
    }

    public static void readProfitData(JavaDStream<String> records) {
        //Generate a pair stream in form of key value pairs for symbol and profit
        JavaPairDStream<String, Double> profitData = records.mapToPair(new PairFunction<String, String, Double>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Double> call(String x) throws Exception {

                //creating mapper object
                ObjectMapper mapper = new ObjectMapper();

                // defining the return type
                TypeReference<Stock> mapType = new TypeReference<Stock>() {
                };

                // Parsing the JSON String
                Stock stock = mapper.readValue(x, mapType);

                return new Tuple2<String, Double>(stock.getSymbol(), stock.getPriceData().getClose() - stock.getPriceData().getOpen());

            }
        });

        //Reduce function for summation of profit amount
        Function2<Double, Double, Double> reduceSumProfit = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double profitPresent, Double profitIncoming) throws Exception {
                System.out.println("Reduce Sum Profit function is running");
                //System.out.println(profitPresent + "+" + profitIncoming);
                return (profitPresent + profitIncoming);
            }
        };

        //Inverse function for subtracting profit amount of the sliding window
        Function2<Double, Double, Double> invSumProfit = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double profitPresent, Double profitOutgoing) throws Exception {
                System.out.println("Inverse Sum Profit function is running");
                //System.out.println("PriceOutgoing= " + profitOutgoing);
                //System.out.println(profitPresent + "-" + profitOutgoing);
                return (profitPresent - profitOutgoing);
            }
        };

        //Reduce function for average of profit amount
        Function2<Double, Double, Double> reduceAvgProfit = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double profitPresent, Double profitIncoming) throws Exception {
                System.out.println("Reduce Avg Profit function is running");
                //System.out.println("Reduce Avg=" + profitIncoming + "/10");
                return (profitIncoming) / 10;
            }
        };

        //Inverse function for subtracting average profit amount of the sliding window
        Function2<Double, Double, Double> invAvgProfit = new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double profitPresent, Double profitOutgoing) throws Exception {
                System.out.println("Inverse Avg Profit function is running");
                //System.out.println("PriceOutgoing= " + profitOutgoing);
                //System.out.println(profitPresent + "-" + "(" + profitOutgoing + ")/10");
                return (profitOutgoing - (profitOutgoing) / 10);
            }
        };

        //Find stock with maximum profit
        Function2<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>> reduceMaxProfit = new Function2<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> maxPresent, Tuple2<String, Double> maxIncoming) throws Exception {
                System.out.println("Reduce Max Profit function is running");
                if (maxPresent._2 >= maxIncoming._2) {  //Using >= implies that in case, two stocks are equal then present stock will take priority
                    return maxPresent;
                } else {
                    return maxIncoming;
                }
            }
        };

        JavaPairDStream<String, Double> sumProfit = profitData.
                reduceByKeyAndWindow(reduceSumProfit, invSumProfit, Durations.seconds(600), Durations.seconds(300));

        JavaPairDStream<String, Double> avgProfit = sumProfit.
                reduceByKeyAndWindow(reduceAvgProfit, invAvgProfit, Durations.seconds(600), Durations.seconds(300));

        JavaDStream<Tuple2<String, Double>> maxProfit = avgProfit.
                reduceByWindow(reduceMaxProfit, Durations.seconds(600), Durations.seconds(300));

        maxProfit.print();
    }

    public static void readVolumeData(JavaDStream<String> records) {
        //Generate a pair stream in form of key value pairs for symbol and trading volume
        JavaPairDStream<String, Integer> volumeData = records.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String x) throws Exception {

                //creating mapper object
                ObjectMapper mapper = new ObjectMapper();

                // defining the return type
                TypeReference<Stock> mapType = new TypeReference<Stock>() {
                };

                // Parsing the JSON String
                Stock stock = mapper.readValue(x, mapType);

                return new Tuple2<String, Integer>(stock.getSymbol(), stock.getPriceData().getVolume());

            }
        });

        //Reduce function for summation of trading volume
        Function2<Integer, Integer, Integer> reduceSumVolume = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer volumePresent, Integer volumeIncoming) throws Exception {
                System.out.println("Reduce Sum Volume function is running");
                //System.out.println(abs(volumePresent) + "+" + abs(volumeIncoming));
                return (abs(volumePresent) + abs(volumeIncoming));
            }
        };

        //Find stock with maximum trading volume
        Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> reduceMaxVolume = new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> maxVolPresent, Tuple2<String, Integer> maxVolIncoming) throws Exception {
                System.out.println("Reduce Max Volume function is running");
                if (maxVolPresent._2 >= maxVolIncoming._2) { //Using >= implies that in case, two stocks are equal then present stock will take priority
                    return maxVolPresent;
                } else {
                    return maxVolIncoming;
                }
            }
        };


        JavaPairDStream<String, Integer> sumVolume = volumeData.
                reduceByKeyAndWindow(reduceSumVolume, Durations.seconds(600));

        JavaDStream<Tuple2<String, Integer>> maxVolumne = sumVolume.
                reduce(reduceMaxVolume);
        maxVolumne.print();

    }

}
