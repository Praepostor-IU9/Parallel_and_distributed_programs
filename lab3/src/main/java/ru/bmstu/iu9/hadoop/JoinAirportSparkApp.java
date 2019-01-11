package ru.bmstu.iu9.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

public class JoinAirportSparkApp {
    private static final int COL_CODE = 0;
    private static final int COL_DESCRIPTION = 1;

    private static final int COL_DELAY = 18;
    private static final int COL_CANCEL = 19;
    private static final int COL_AIRPORT_OUT = 11;
    private static final int COL_AIRPORT_IN = 14;

    public static void main(String[] args) {
//        if (args.length != 3) {
//            System.err.println("Usage: JoinAirportSparkApp <input path flight> <input path airport> <output path>");
//            System.exit(1);
//        }

        SparkConf conf = new SparkConf();
        conf.setAppName("lab3");
        //conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flights = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airport = sc.textFile("L_AIRPORT_ID.csv");

        JavaPairRDD<Integer, String> codeDescription = airport
                .filter(line -> line.charAt(0) != 'C')
                .mapToPair((PairFunction<String, Integer, String>) s -> {
                    String[] col = s.split("\",");
                    Integer code = Integer.parseInt(col[COL_CODE].replaceFirst("\"", ""));
                    String name = col[COL_DESCRIPTION].replaceAll("\"", "");
                    return Tuple2.apply(code, name);
                });
        Broadcast<Map<Integer, String>> codeBroadcast = sc.broadcast(codeDescription.collectAsMap());

        JavaPairRDD<Tuple2<Integer, Integer>, String> flightInfo = flights
                .filter(line -> line.charAt(0) != '\"')
                .mapToPair((PairFunction<String, Tuple2<Integer, Integer>, Tuple2<Float, Boolean>>) s -> {
                    String[] col = s.split(",");
                    Boolean flag = Float.parseFloat(col[COL_CANCEL]) > 0.5f;
                    Float delay = (col[COL_DELAY].equals(""))? 0.0f : Float.parseFloat(col[COL_DELAY]);
                    Integer outID = Integer.parseInt(col[COL_AIRPORT_OUT]);
                    Integer inID = Integer.parseInt(col[COL_AIRPORT_IN]);
                    return Tuple2.apply(Tuple2.apply(outID, inID), Tuple2.apply(delay, flag));
                })
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Float, Boolean>>>, Tuple2<Integer, Integer>, String>) tuple2IterableTuple2 -> {
                    Iterator<Tuple2<Float, Boolean>> iter = tuple2IterableTuple2._2.iterator();
                    float max = 0.0f;
                    int sumCancel = 0;
                    int sumLate = 0;
                    int size = 0;
                    while(iter.hasNext()) {
                        Tuple2<Float, Boolean> tup = iter.next();
                        float delay = tup._1;
                        boolean flag = tup._2;
                        if (delay > max)
                            max = delay;
                        if (delay > 1/600 && !flag)
                            sumLate++;
                        if (flag)
                            sumCancel++;
                        size++;
                    }
                    String info = String.format("Максимальное время опоздания: %.1f, " +
                                    "Процент опоздания: %.1f, " +
                                    "Процент отмен: %.1f, " +
                                    "Процент опозданий и отмен: %.1f",
                            max,
                            ((float) sumLate / size) * 100,
                            ((float) sumCancel / size) * 100,
                            ((float) (sumLate + sumCancel) / size) * 100);
                    return Tuple2.apply(tuple2IterableTuple2._1, info);
                });
        JavaPairRDD<Tuple2<String, String>, String> join = flightInfo
                .mapToPair((PairFunction<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<String, String>, String>) tuple2StringTuple2 -> Tuple2.apply(
                        Tuple2.apply(codeBroadcast.value().get(tuple2StringTuple2._1._1),
                                codeBroadcast.value().get(tuple2StringTuple2._1._2)),
                        tuple2StringTuple2._2));
        join.saveAsTextFile("output");
    }
}
