package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.HashMap;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Stemmer stemmer = new Stemmer();
        private final Text mapKey = new Text();

        @Override // experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] rowParts = value.toString().split("\t");
            String[] wordNodes = rowParts[1].split(" ");

            // the 0th is empty in these
            String[] words = new String[4];
            String[] descriptions = new String[4];
            int[] destinations = new int[4];

            int i = 1;
            for(String word : wordNodes) {
                String[] wordParts = word.split("/");

                String unStemmedWord = wordParts[0];
                String edgeDescription = wordParts[2];
                int edgeDestination = Integer.parseInt(wordParts[3]);

                stemmer.add(unStemmedWord.toCharArray(),unStemmedWord.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                words[i] = stemmedWord;
                descriptions[i] = edgeDescription;
                destinations[i] = edgeDestination;

                i++;
            }

            for(i = 1; i < 4; i++) {
                mapKey.set("$" + words[i]);
                // count(l)
                context.write(mapKey, one);

                // count(L)
                mapKey.set("*L1");
                context.write(mapKey, one);
                mapKey.set("*L2");
                context.write(mapKey, one);
                mapKey.set("*L3");
                context.write(mapKey, one);

                if(destinations[i] != 0) { // if there's an edge (not root)
                    String wordDesc = words[i] + "-" + descriptions[i];

                    // count(f)
                    mapKey.set(wordDesc);
                    context.write(mapKey, one);

                    // count(l,f)
                    mapKey.set(words[destinations[i]] + "#" + wordDesc);
                    context.write(mapKey, one);

                    // count(F)
                    mapKey.set("*F1");
                    context.write(mapKey, one);
                    mapKey.set("*F2");
                    context.write(mapKey, one);
                    mapKey.set("*F3");
                    context.write(mapKey, one);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text, DoubleWritable> {
        double L = 0;
        double F = 0;
        public HashMap<String,Double> hashMap = new HashMap<>(); // l, sum

        @Override// (*L3,[1,1,1,1,1,...])
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            // lexicographically * -> $ -> else.
            double sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if(key.charAt(0) == '*') {
                if(key.charAt(1) == 'L')
                    L = sum;
                if(key.charAt(1) == 'F')
                    F = sum;
            } else if(key.charAt(0) == '$') { //p(l)
                double prob = sum / L;
                String lexema = key.toString().substring(1);
                hashMap.put(lexema,sum);
                context.write(new Text(lexema), new DoubleWritable(prob));
            } else {
                // we enter if its l#f
                if(key.find("#") != -1) { //p(l,f), p(f | l)
                    String l = key.toString().split("#")[0];
                    String f = key.toString().split("#")[1];

                    double probLf = sum / L; // p(l,f)
                    context.write(new Text(l+","+f),new DoubleWritable(probLf));

                    double probLGivenF = sum / hashMap.get(l); // p(f | l) = count(l,f) / count(L=l)
                    context.write(new Text(l+"|"+f), new DoubleWritable(probLGivenF));
                } else { //p(f)
                    double prob = sum / F;
                    context.write(key, new DoubleWritable(prob));
                }
            }
        }
    }



    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            //so that every reducer will receive *F or *L for sure.
            if(key.charAt(0) == '*') { // *F1 , *L2
                return key.charAt(2);
            }
            return Math.abs(key.hashCode()) % 3;
        }
    }
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        // start with a smaller file
        TextInputFormat.addInputPath(job, new Path("s3://biarcs/0.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule/probs.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}