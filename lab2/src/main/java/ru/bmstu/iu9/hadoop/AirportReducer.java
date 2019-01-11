package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airportName = new Text("Airport name: " + iter.next().toString());
        if (iter.hasNext()) {
            int i = 1;
            float delay = Float.parseFloat(iter.next().toString());
            float min = delay;
            float max = delay;
            float mean = delay;
            while (iter.hasNext()) {
                delay = Float.parseFloat(iter.next().toString());
                mean += delay;
                if (delay > max)
                    max = delay;
                if (delay < min)
                    min = delay;
                i++;
            }
            mean /= i;
            Text minMaxMean = new Text("Min: "+Float.toString(min)+
                    ", Max: "+Float.toString(max)+
                    ", Mean: "+Float.toString(mean));
            context.write(airportName, minMaxMean);
        }
    }
}
