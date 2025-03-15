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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Step5 {
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
            System.out.println("[DEBUG] firstToSecond: " + firstToSecond);
            System.out.println("[DEBUG] secondToFirst: " + secondToFirst);
        }

        @Override //format is: l  5   [0.32, 0.52, 0.456, 0.65...0.1] (length 1100)
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            //check if l is in first or in second.
            //if it is in first, check if the pair is in the gold standard
            String[] parts = value.toString().split("\t");
            String lexeme = parts[0];
            String vecNum = parts[1];
            String vec = parts[2];

            if(firstToSecond.containsKey(lexeme)) { //alig-frog
                //for each second, send first-second with the value and the map[first][second]
                for(String second : firstToSecond.get(lexeme).keySet()) {//alig-frog
                    mapKey.set(lexeme + "-" + second);
                    //format: alig-frog alig  5   [0.32, 0.52, 0.456, 0.65]    true
                    context.write(mapKey, new Text(lexeme + "\t" + vecNum + "\t" + vec + "\t" + firstToSecond.get(lexeme).get(second).toString()));
                }
            }

            if(secondToFirst.containsKey(lexeme)) { //frog   alig-frog
                //for each second, send first-second with the value and the map[first][second]
                for(String first : secondToFirst.get(lexeme).keySet()) {
                    mapKey.set(first + "-" + lexeme);
                    //format: alig-frog  frog  5   [0.32, 0.52, 0.456, 0.65]    true
                    context.write(mapKey, new Text(lexeme + "\t" + vecNum + "\t" + vec + "\t" + secondToFirst.get(lexeme).get(first).toString()));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text,Text,Text,NullWritable> {


        @Override //format: first-second   {"first  5   [0.32, 0.52, 0.456, 0.65...]  true", "second  5   [0.32, 0.41, 0.3, 0.5]  true"}
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String first = key.toString().split("-")[0];//alig
            String second = key.toString().split("-")[1];//frog

            List<List<Double>> firstVecs = new ArrayList<>(Arrays.asList(null, null, null, null));
            List<List<Double>> secondVecs = new ArrayList<>(Arrays.asList(null, null, null, null));

            int featuresSize = 0;
            String bool = "bobba"; // no particular reason for "false" base value

            for (Text value : values) {
                String[] row = value.toString().split("\t");
                String word = row[0];//first
                int vecNum = Integer.parseInt(row[1]);//5, 6, 7, 8
                String vector = row[2];//[0.32, 0.52, 0.456, 0.65]
                bool = row[3];//true


                // Convert String [0.34, 0.65, 0.23] to List<Double>
                List<Double> vectorList = Arrays.stream(vector.replaceAll("[\\[\\]]", "") // Remove brackets
                                .split(",\\s*")) // Split by comma and optional space
                        .map(Double::parseDouble) // Convert to double
                        .collect(Collectors.toList());
                featuresSize = vectorList.size();

                if (word.equals(first)) {
                    firstVecs.set(vecNum - 5, vectorList);
                } else if (word.equals(second)) {
                    secondVecs.set(vecNum - 5, vectorList);//5, [0.34, 0.65, 0.23]
                }
            }

            if(firstVecs.contains(null) || secondVecs.contains(null)) {
                return;
            }


            // Calculate similarity
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
                    double currdLeft17 =  valVec1 * Math.log(valVec1/((valVec1 + valVec2) / 2));
                    double currdRight17 = valVec2 * Math.log(valVec2/((valVec1 + valVec2) / 2));
                    if(!Double.isFinite(currdLeft17)) {
                        currdLeft17 = 0;
                    }
                    if(!Double.isFinite(currdRight17)) {
                        currdRight17 = 0;
                    }
                    dLeft17 += currdLeft17;
                    dRight17 += currdRight17;
                }
                //9
                vectorSimilarity.add(i * 6, val9);
                //10
                val10 = Math.sqrt(val10);
                vectorSimilarity.add(i * 6 + 1, val10);
                // 11
                val11denominatorLeft = Math.sqrt(val11denominatorLeft);
                val11denominatorRight = Math.sqrt(val11denominatorRight);
                double val11;
                if(!Double.isFinite(val11denominatorLeft) || val11denominatorLeft == 0.0 || !Double.isFinite(val11denominatorRight) || val11denominatorRight == 0.0 )
                    val11 = 0.0;
                else
                    val11 = val11numerator / (val11denominatorLeft * val11denominatorRight);

                vectorSimilarity.add(i * 6 + 2, val11);

                //13
                double val13;
                if(val13Max == 0.0) {
                    val13 = 0.0;
                } else  {
                    val13 = val13Min15 / val13Max;
                }
                vectorSimilarity.add(i * 6 + 3, val13);

                //15
                double val15;
                if(val15sum == 0.0) {
                    val15 = 0.0;
                } else {
                    val15 = 2 * val13Min15 / val15sum;
                }
                vectorSimilarity.add(i * 6 + 4, val15);

                //17
                double val17 = dLeft17 + dRight17;
                vectorSimilarity.add(i * 6 + 5, val17);
            }
            //"hello", "world", 0.1, 0.5, ..., True
            StringBuilder output = new StringBuilder();
//            output.append("\"").append(first).append("\", ");
//            output.append("\"").append(second).append("\", ");

            // Format the values as a string
            for (Double val : vectorSimilarity) {
                output.append(val.toString()).append(", ");
            }
            output.append(bool);
            context.write(new Text(output.toString().trim()), NullWritable.get());// "hello", "world", 0.1, 0.5, ..., True
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/dataset"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/dataset_similarity"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

