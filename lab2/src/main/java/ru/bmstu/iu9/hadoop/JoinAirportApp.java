package ru.bmstu.iu9.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinAirportApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("USage: JoinAirportApp <input path flight> <input path airport> <output path>");
            System.exit(1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(JoinAirportApp.class);
        job.setJobName("Join Airport table");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(AirportReducer.class);
        job.setPartitionerClass(AirportPartitioner.class);
        job.setGroupingComparatorClass(AirportComparator.class);

        job.setMapOutputKeyClass(AirportWritableComparable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}