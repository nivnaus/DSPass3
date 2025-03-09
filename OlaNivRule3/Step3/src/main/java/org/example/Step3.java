package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();
        private HashMap<String,HashMap<String, Boolean>> firstToSecond = new HashMap<>();
        private HashMap<String, HashMap<String, Boolean>> secondToFirst = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // load gold standard from local (Text file called "word-relatedness.txt")

            String fileName = "word-relatedness.txt";

            try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
                assert inputStream != null;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String first = parts[0];
                        String second = parts[1];
                        Boolean bool = Boolean.parseBoolean(parts[2]);

                        if (!firstToSecond.containsKey(first)) {
                            firstToSecond.put(first, new HashMap<>());
                        }
                        firstToSecond.get(first).put(second, bool);

                        if (!secondToFirst.containsKey(second)) {
                            secondToFirst.put(second, new HashMap<>());
                        }
                        secondToFirst.get(second).put(first, bool);
                    }
                }
            } catch (IOException e) {
                //we do nothing with it.
            }
        }

        @Override //format is: l  5   [0.32, 0.52, 0.456, 0.65]
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //check if l is in first or in second.
            //if it is in first, check if the pair is in the gold standard
            String[] parts = value.toString().split("\t");
            String lexeme = parts[0];
            String vecNum = parts[1];
            String vec = parts[2];

            if(firstToSecond.containsKey(lexeme)) {
                //for each second, send first-second with the value and the map[first][second]
                for(String second : firstToSecond.get(lexeme).keySet()) {
                    mapKey.set(lexeme + "-" + second);
                    //format: lexeme-second lexeme  5   [0.32, 0.52, 0.456, 0.65] true
                    context.write(mapKey, new Text(lexeme + "\t" + vecNum + "\t" + vec + "\t" + firstToSecond.get(lexeme).get(second)));
                }
            }

            if(secondToFirst.containsKey(lexeme)) {
                //for each second, send first-second with the value and the map[first][second]
                for(String first : secondToFirst.get(lexeme).keySet()) {
                    mapKey.set(first + "-" + lexeme);
                    //format: first-lexeme  lexeme  5   [0.32, 0.52, 0.456, 0.65] true
                    context.write(mapKey, new Text(lexeme + "\t" + vecNum + "\t" + vec + "\t" + secondToFirst.get(first).get(lexeme)));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,NullWritable> {

        @Override //format: lexeme-second   {lexeme  5   [0.32, 0.52, 0.456, 0.65] true}
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String first = key.toString().split("-")[0];
            String second = key.toString().split("-")[1];

            List<List<Double>> firstVecs = new ArrayList<>(4);
            List<List<Double>> secondVecs = new ArrayList<>(4);

            int featuresSize = 0;
            String[] row = key.toString().split("\t");
            String word = row[0];
            int vecNum = Integer.parseInt(row[1]);
            String vector = row[2];
            String bool = row[3];

            for (Text value : values) {

                // Convert String [0.34, 0.65, 0.23] to List<Double>
                List<Double> vectorList = Arrays.stream(vector.replaceAll("[\\[\\]]", "") // Remove brackets
                                .split(",\\s*")) // Split by comma and optional space
                        .map(Double::parseDouble) // Convert to double
                        .collect(Collectors.toList());
                featuresSize = vectorList.size();

                if (word.equals(first)) {
                    firstVecs.add(vecNum - 5, vectorList);
                } else if (word.equals(second)) {
                    secondVecs.add(vecNum - 5, vectorList);
                }
            }

            // [5x9, 5x10, ..., 6x9, 6x10,...]
            List<Double> vectorSimilarity = new ArrayList<>(24);

            for (int i = 0; i < 4; i++) {

                List<Double> vec1 = firstVecs.get(i);
                List<Double> vec2 = secondVecs.get(i);

                //9
                double val9 = 0;
                //10
                double val10 = 0;
                //11
                double val11numerator = 0;
                double val11denominatorLeft = 0;
                double val11denominatorRight = 0;
                //13
                double val13Min15 = 0;
                double val13Max = 0;
                //15
                double val15sum = 0;
                //17
                double dLeft17 = 0;
                double dRight17 = 0;

                for (int j = 0; j < featuresSize; j++) {

                    double valVec1 = vec1.get(j);
                    double valVec2 = vec2.get(j);
                    //9
                    val9 += Math.abs(valVec1 - valVec2);
                    //10
                    val10 += Math.pow(valVec1 - valVec2, 2);
                    //11
                    val11numerator += valVec1 * valVec2;
                    val11denominatorLeft += Math.pow(valVec1, 2);
                    val11denominatorRight += Math.pow(valVec2, 2);
                    //13
                    val13Min15 += Math.min(valVec1, valVec2);
                    val13Max += Math.max(valVec1, valVec2);
                    //15
                    val15sum += valVec1 + valVec2;
                    //17
                    dLeft17 += valVec1 * Math.log(valVec1/((valVec1 + valVec2) / 2));
                    dRight17 += valVec2 * Math.log(valVec2/((valVec1 + valVec2) / 2));
                }
                //9
                vectorSimilarity.add(i * 6, val9);
                //10
                val10 = Math.sqrt(val10);
                vectorSimilarity.add(i * 6 + 1, val10);
                // 11
                val11denominatorLeft = Math.sqrt(val11denominatorLeft);
                val11denominatorRight = Math.sqrt(val11denominatorRight);
                double val11 = val11numerator / (val11denominatorLeft * val11denominatorRight);
                vectorSimilarity.add(i * 6 + 2, val11);

                //13
                double val13 = val13Min15 / val13Max;
                vectorSimilarity.add(i * 6 + 3, val13);

                //15
                double val15 = 2 * val13Min15 / val15sum;
                vectorSimilarity.add(i * 6 + 4, val15);

                //17
                double val17 = dLeft17 + dRight17;
                vectorSimilarity.add(i * 6 + 5, val17);
            }
            //"hello", "world", 0.1, 0.5, ..., True
            StringBuilder output = new StringBuilder();
            output.append("\"").append(first).append("\", ");
            output.append("\"").append(second).append("\", ");

            // Format the values as a string
            for (Double val : vectorSimilarity) {
                output.append(val.toString()).append(", ");
            }
            output.append(bool);
            context.write(new Text(output.toString().trim()), NullWritable.get());
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % 7;
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
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/landf.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/dataset.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}