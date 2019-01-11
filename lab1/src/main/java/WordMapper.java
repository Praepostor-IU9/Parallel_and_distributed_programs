import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.toLowerCase().split("[ \f\n\r\t,.:;\"?!+=()\\[\\]«»{}<>…]+");
        for (String word : words) {
            if (word == "") continue;
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
