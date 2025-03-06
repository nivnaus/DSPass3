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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Stemmer stemmer = new Stemmer();
        private final Text mapKey = new Text();

        @Override // experience     that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
        // "^   i/FW/nn/3 i/FW/nn/3 "^/FW/dep/0 28	2000,28
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            System.out.println(value.toString());
            String[] rowParts = value.toString().split("\t");
            String[] wordNodes = rowParts[1].split(" ");

            // the 0th is empty in these
            List<String> words = new ArrayList<>();
            List<String> descriptions = new ArrayList<>();
            List<Integer> destinations = new ArrayList<>();

            int wordNodesSize = 0;
            for(String word : wordNodes) { //3
                String[] wordParts = word.split("/");

                int bias = 0;
                String unStemmedWord;
                String edgeDescription;
                int edgeDestination;

                //todo: fix later
                if(wordParts.length == 4) { //that/IN/compl/3
                    unStemmedWord = wordParts[bias];
                } else if(wordParts.length == 5) { //  //IN/compl/3
                    bias = 1;
                    unStemmedWord = "/";
                } else  { // ////ROOT/0 todo: i hope that this is the only 'else'
                    bias = wordParts.length - 4;
                    unStemmedWord = "/";
                }

                edgeDescription = wordParts[2 + bias];
                edgeDestination = Integer.parseInt(wordParts[3 + bias]);


                stemmer.add(unStemmedWord.toCharArray(),unStemmedWord.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                words.add(stemmedWord);
                descriptions.add(edgeDescription);
                destinations.add(edgeDestination);

                wordNodesSize++;
            }

            for(int j = 0; j < wordNodesSize; j++) {
                mapKey.set("$" + words.get(j));
                // count(l)
                context.write(mapKey, one);

                // count(L)
                mapKey.set("*L1");
                context.write(mapKey, one);
                mapKey.set("*L2");
                context.write(mapKey, one);
                mapKey.set("*L3");
                context.write(mapKey, one);

                if(destinations.get(j) != 0) { // if there's an edge (not root)
                    String wordDesc =words.get(j) + "-" + descriptions.get(j);

                    // count(f)
                    mapKey.set(wordDesc);
                    context.write(mapKey, one);

                    // count(l,f)
                    mapKey.set(words.get(destinations.get(j) - 1) + "#" + wordDesc);
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

                    // l = *h , l = $*h  , l|f -> *h|dog-subj
                    //todo: null pointer exception here 6.3 22:48
                    // can be because some words are starting for example in * , and then its before $, so l doesnt exist yet in the map.
                    // f that
                    // option: fix so that words/features that continue are only alphabetical.
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
            List<String> FAndL = Arrays.asList("*F1", "*F2", "*F3", "*L1", "*L2", "*L3");
            if(FAndL.contains(key.toString())){
                return key.charAt(2) - '1';
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
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        // start with a smaller file
        // 0-98.txt
        TextInputFormat.addInputPath(job, new Path("s3://biarcs/0.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/probs.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}