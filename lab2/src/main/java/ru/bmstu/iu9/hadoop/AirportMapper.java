package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {

    private static final int COL_Code = 0;
    private static final int COL_Description = 1;
    private static final byte FLAG_AIRPORT = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] colums = value.toString().split("\",");
        if (key.get() > 0) {
            Text airportName = new Text(colums[COL_Description].replaceAll("\"", ""));
            int airportCode = Integer.parseInt(colums[COL_Code].replaceAll("\"", ""));
            context.write(new AirportWritableComparable(airportCode, FLAG_AIRPORT), airportName);
        }
    }
}
