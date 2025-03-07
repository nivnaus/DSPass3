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
import java.util.*;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Stemmer stemmer = new Stemmer();
        private final Text mapKey = new Text();
//        private final HashSet<String> stopWords = new HashSet<>(Arrays.asList(
//                "״", "׳", "־", "^", "?", ";", ":", ".", "-", "*", "\"", "!"
//                , ")", "(", "#", "$", "%", "&", "'", "+", ",", "/"
//                , "=", "@", "[", "]", "{", "}", "|", "<", ">", "_", "`", "~"
//        ));
        // $	$/$/pobj/0 $/$/conj/1 $/$/conj/1 as/RB/mwe/5 well/RB/cc/1 as/IN/mwe/5	11	1994,3	1997,3	2002,2	2003,3
             // experience     that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            String[] rowParts = value.toString().split("\t");
            String[] wordNodes = rowParts[1].split(" "); // $///pobj/0 asd/asd/conj/1 as/RB/mwe/5 well/RB/cc/1 $/$/mwe/5

            List<String> words = new ArrayList<>();
            List<String> descriptions = new ArrayList<>();
            List<Integer> destinations = new ArrayList<>();

            int wordNodesSize = 0;
            for (String wordNode : wordNodes) {
                String[] wordParts = wordNode.split("/");

                if (wordParts.length > 4) return; // not //
                String rawWord = wordParts[0]; // Extract the actual word (before first `/`)
                String secondParam = wordParts[1];

                // **Skip if it's a stop word**
                if (!rawWord.matches("[a-zA-Z]+") || !secondParam.matches("[a-zA-Z]+")) return;

                // **Process valid words**
                String edgeDescription = wordParts[2];
                int edgeDestination = Integer.parseInt(wordParts[3]);

                // Apply stemming
                stemmer.add(rawWord.toCharArray(), rawWord.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                words.add(stemmedWord);
                descriptions.add(edgeDescription);
                destinations.add(edgeDestination);
                wordNodesSize++;
            }

            for(int j = 0; j < wordNodesSize; j++) {
                mapKey.set("+" + words.get(j)); //words: 0,that 1,patients 2,experience size: 3
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

                    //j = 0 -> expericnce#that-compl
                    //words[2]#
                    //that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0
                    //expericnce#that-compl, experience#patients-nsubj
                    // count(l,f) //words: 0,that 1,patients 2,experience size: 3
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
            // lexicographically * -> + -> else.
            double sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }


            if(key.toString().charAt(0) == '*') {// TODO - need to decide if toString is needed.
                if(key.toString().charAt(1) == 'L')// TODO - need to decide if toString is needed.
                    L = sum;
                if(key.toString().charAt(1) == 'F')// TODO - need to decide if toString is needed.
                    F = sum;
            } else if(key.toString().charAt(0) == '+') { //p(l) - TODO - need to decide if toString is needed.
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
            List<String> FAndL = Arrays.asList("*F1", "*F2", "*F3", "*L1", "*L2", "*L3");
            if(FAndL.contains(key.toString())){// TODO - need to decide if toString is needed.
                return key.charAt(2) - '1';
            }
            if(key.find("#") != -1) { // $l, l#f we want $l and l#f to be together -> isolate $l in both
                String l = "+" + key.toString().split("#")[0];
                return Math.abs(l.hashCode()) % 3;
            }

            return Math.abs(key.toString().hashCode()) % 3;
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