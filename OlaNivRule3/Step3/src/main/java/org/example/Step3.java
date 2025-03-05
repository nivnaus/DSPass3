package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        String[] parsed = value.toString().split("\t");
        String trio = parsed[0];
        int trioSum = Integer.parseInt(parsed[1]);

        if((!trio.equals("*#*#*") && trio.contains("*#*#")) || !(trio.contains("*"))) {
            mapKey.set(trio);
            context.write(mapKey, new IntWritable(trioSum));
        }
    }
}

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
        private HashMap<String, Integer> asteriskMap = new HashMap<>();// *#*#w3 *#w2#* w1#*#*

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            String trio = key.toString();
            String[] parsedTrio = trio.split("#");
            String w1 = parsedTrio[0];
            String w2 = parsedTrio[1];
            String w3 = parsedTrio[2];
            int freqOfTrio = values.iterator().next().get(); //values should have exactly one value

            if(w1.equals("*") && w2.equals("*")) {
                asteriskMap.put(trio,freqOfTrio);
            } else {
                //collect n1
                int n1 = asteriskMap.get("*#*#"+w3);
                context.write(key, new Text("n1#"+n1));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Partition by the third word in the trio
            String[] parsedTrio = key.toString().split("#");
            String w3 = parsedTrio[2];
            return Math.abs(w3.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/constsW3.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}