package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {

    private static final int COL_DEST_AEROPORT_ID = 14;
    private static final int COL_ARR_DELAY_NEW = 18;
    private static final byte FLAG_FLIGHT = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] colums = value.toString().split(",");
        if (key.get() > 0) {
            float delay = (colums[COL_ARR_DELAY_NEW].equals(""))? 0.0f : Float.parseFloat(colums[COL_ARR_DELAY_NEW]);
            if (delay > 1/600) {
                int airportID = Integer.parseInt(colums[COL_DEST_AEROPORT_ID]);
                context.write(new AirportWritableComparable(airportID, FLAG_FLIGHT), new Text(colums[COL_ARR_DELAY_NEW]));
            }
        }
    }
}
