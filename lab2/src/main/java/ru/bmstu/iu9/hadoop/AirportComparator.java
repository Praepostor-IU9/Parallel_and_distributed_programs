package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportComparator extends WritableComparator {
    protected AirportComparator() {
        super(AirportWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirportWritableComparable that = (AirportWritableComparable) a;
        AirportWritableComparable other = (AirportWritableComparable) b;
        return Integer.compare(that.getAirportID(), other.getAirportID());
    }
}
