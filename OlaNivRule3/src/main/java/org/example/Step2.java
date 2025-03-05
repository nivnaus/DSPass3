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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class Step2 {
    public static long c0 = 0;

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();
//        long c0;
//
//        @Override
//        protected void setup(Context context) throws IOException {
//            c0 = 0;
//        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //nothing special, just count c0
            // input line: *#*#yeled 24
            String[] parsed = value.toString().split(" "); //todo: maybe change
            String trio = parsed[0];
            int trioSum = Integer.parseInt(parsed[1]);
            //sum everything that has 2 starts for C0
            // *#*#w3 *#w2#* w1#*#*
            if(trio.indexOf('*') != trio.lastIndexOf('*')) {
                c0 += trioSum;
            }
            String[] parsedTrio = trio.split("#");
            String w2 = parsedTrio[1];
            String w3 = parsedTrio[2];
            if (w2.equals("*") && w3.equals("*")) { // if not w1#*#*
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

            if(w1.equals("*") || w3.equals("*")) {
                asteriskMap.put(trio,freqOfTrio);
            } else {
                //collect n2, c1, c2..
                int c1 = asteriskMap.get("*#"+w2+"#*");
                int c2 = asteriskMap.get(w1+w2+"#*");
                int n2 = asteriskMap.get("*#"+w2+"#"+w3);
                int n3 = freqOfTrio;

                context.write(key, new Text("c0#"+c0));
                context.write(key, new Text("c1#"+c1));
                context.write(key, new Text("c2#"+c2));
                context.write(key, new Text("n2#"+n2));
                context.write(key, new Text("n3#"+n3));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Partition by the second word in the trio
            String[] parsedTrio = key.toString().split("#");
            String w2 = parsedTrio[1];
            return w2.hashCode() % numPartitions;
        }
    }

    // TODO: we need to change it for our assignment
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.MapperClass.class);
        job.setPartitionerClass(Step2.PartitionerClass.class);
        job.setCombinerClass(Step2.ReducerClass.class);
        job.setReducerClass(Step2.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/consts.txt"));// TODO: change this to our own bucket
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
