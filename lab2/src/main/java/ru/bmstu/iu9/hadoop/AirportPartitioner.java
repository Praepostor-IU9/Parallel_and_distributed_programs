package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportPartitioner extends Partitioner<AirportWritableComparable, Text> {
    @Override
    public int getPartition(AirportWritableComparable airportWritableComparable, Text text, int i) {
        return airportWritableComparable.getAirportID() % i;
    }
}
