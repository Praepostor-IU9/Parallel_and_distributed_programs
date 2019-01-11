package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AirportWritableComparable implements WritableComparable {

    private int airportID;
    private byte flag;

    public AirportWritableComparable() {}

    public AirportWritableComparable(int airportID, byte flag) {
        this.airportID = airportID;
        this.flag = flag;
    }

    public int getAirportID() {
        return airportID;
    }

    public void setAirportID(int airportID) {
        this.airportID = airportID;
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AirportWritableComparable that = (AirportWritableComparable) o;
        return airportID == that.airportID &&
                flag == that.flag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(airportID, flag);
    }

    @Override
    public String toString() {
        return "AirportWritableComparable{" +
                "airportID=" + airportID +
                ", flag=" + flag +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        AirportWritableComparable that = (AirportWritableComparable) o;
        if (this.airportID > that.airportID)
            return 1;
        else if (this.airportID < that.airportID)
            return -1;
        if (this.flag > that.flag)
            return 1;
        else if (this.flag < that.flag)
            return -1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportID);
        dataOutput.writeByte(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportID = dataInput.readInt();
        flag = dataInput.readByte();
    }
}
