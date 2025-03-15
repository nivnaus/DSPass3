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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Stemmer stemmer = new Stemmer();
        private final Text mapKey = new Text();
        // HashMap of golden standard
        HashSet<String> goldenStandard = new HashSet<>(20000);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // load gold standard from local (Text file called "word-relatedness.txt")
            String fileName = "word-relatedness.txt";
            try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
                assert inputStream != null;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] words = line.split("\t");// alig   frog  true
                        goldenStandard.add(words[0]);
                        goldenStandard.add(words[1]);
                    }
                }
            }
        }


        @Override// experience     that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            String[] rowParts = value.toString().split("\t");
            String[] words = rowParts[1].split(" ");// [thats/IN/compl/3, patients/NNS/nsubj/3, experience/VB/ccomp/0]
            IntWritable totalCount = new IntWritable(Integer.parseInt(rowParts[2]));// 3092
            boolean containLexeme = false;
            for(String word : words){// thats/IN/compl/3
                String[] wordParts = word.split("/");// [that, IN, compl, 3]
                if (wordParts.length > 4) return; // not // not a word
                String rawWord = wordParts[0];// thats -> that
                String secondParam = wordParts[1];
                if (!rawWord.matches("[a-zA-Z]+") || !secondParam.matches("[a-zA-Z]+")) return;
                stemmer.add(rawWord.toCharArray(), rawWord.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();
                // **Skip if it's a stop word**
                // aligator, aligators, aligator's -> aligator

                if(goldenStandard.contains(stemmedWord)){// if the word is in the golden standard
                    mapKey.set("***L");
                    context.write(mapKey, totalCount);
                    containLexeme = true;
                }
            }

            if(containLexeme) {
                mapKey.set(rowParts[1]);// thats/IN/compl/3 *a&/NNS/nsubj/3 experience/VB/ccomp/1
                context.write(mapKey, totalCount);
            }
                //for each word I want to emit the feature of it, with the ***F key
                for(String word : words){// that/IN/compl/3
                    String[] wordParts = word.split("/");// [that, IN, compl, 3]
                    int destination = Integer.parseInt(wordParts[3]);// 3
                    if(destination != 0){
                        mapKey.set("***F");
                        context.write(mapKey, totalCount);
                        String rawWord = wordParts[0];
                        String secondParam = wordParts[1];
                        if (!rawWord.matches("[a-zA-Z]+") || !secondParam.matches("[a-zA-Z]+")) return;
                        stemmer.add(rawWord.toCharArray(), rawWord.length());
                        stemmer.stem();
                        String stemmedWord = stemmer.toString();
                        mapKey.set(stemmedWord);//unstemmed words, also garbage words *a&-subj,
                        context.write(mapKey, totalCount);// thats 3092
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // lexicographically * -> + -> else.

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // if 3 *
            if(key.toString().equals("***L")){
                String s3Path = "s3://nivolarule05032025/L.txt";
                writeToS3(context,s3Path, sum);
            }
            else if(key.toString().equals("***F")){
                String s3Path = "s3://nivolarule05032025/F.txt";
                writeToS3(context,s3Path, sum);
            }
            else{
                if(key.toString().contains(("/"))){
                    multipleOutputs.write("lineOfLexemesInGS", key, new IntWritable(sum), "lineOfLexemesInGS/part");
                }
                else{
                    multipleOutputs.write("features", key, new IntWritable(sum), "features/part");
                }
            }
        }

        private void writeToS3(Context context, String s3Path, int sum) throws IOException {
            Configuration conf = context.getConfiguration();
            Path path = new Path(s3Path);
            FileSystem fs = FileSystem.get(path.toUri(), conf); // Automatically uses the Hadoop configuration

            try (OutputStream outputStream = fs.create(path);
                 PrintWriter writer = new PrintWriter(outputStream)) {
                writer.println(sum);
            }
        }

        @Override
        protected void cleanup(Reducer < Text, IntWritable, Text, IntWritable >.Context context) throws
        IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
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
        job.setOutputValueClass(IntWritable.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        // start with a smaller file
        // 0-9.txt -> 10%
        for(int i = 0; i < 10; i++){
            TextInputFormat.addInputPath(job, new Path("s3://biarcs/" + i + ".txt"));
        }
        // 0-98.txt -> 100%
//        for (int i = 0; i < 99; i++){
//            TextInputFormat.addInputPath(job, new Path("s3://biarcs/" + i + ".txt"));
//        }

//        TextInputFormat.addInputPath(job, new Path("s3://biarcs/0.txt")); // 0.txt -> 1%
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/output"));
        MultipleOutputs.addNamedOutput(job, "lineOfLexemesInGS", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "features", TextOutputFormat.class, Text.class, IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}