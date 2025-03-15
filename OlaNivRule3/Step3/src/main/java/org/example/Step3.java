package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class Step3 {// a/DT/ROOT/0 and/CC/cc/1 b/NN/conj/1 e/LS/dobj/3  12
    //
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        Stemmer stemmer = new Stemmer();
        private final Text mapKey = new Text();
        private final HashMap<String, Integer> MostCommonFeatures = new HashMap<>();// feature, count
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
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            ListObjectsV2Result result = s3.listObjectsV2("nivolarule05032025", "filtered_features/");
            List<S3ObjectSummary> objects = result.getObjectSummaries();
            for (S3ObjectSummary os : objects) {
                S3Object s3object = s3.getObject("nivolarule05032025", os.getKey());
                BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length != 2) return; // Skip malformed lines
                    try {
                        String feature = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        MostCommonFeatures.put(feature, count);
                    } catch (NumberFormatException e) {
                        // Ignore lines where the count is not a valid integer
                    }
                }
            }
        }

        // that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println(value.toString());
            String[] rowParts = value.toString().split("\t");
            String[] wordNodes = rowParts[0].split(" ");// [thats/IN/compl/3, patients/NNS/nsubj/3, experience/VB/ccomp/0]
            IntWritable totalCount = new IntWritable(Integer.parseInt(rowParts[1]));// 3092

            List<String> words = new ArrayList<>();
            List<String> descriptions = new ArrayList<>();
            List<Integer> destinations = new ArrayList<>();

            int wordNodesSize = 0;
            for (String wordNode : wordNodes) {
                String[] wordParts = wordNode.split("/");

                String rawWord = wordParts[0]; // Extract the actual word (before first `/`)

                // **Process valid words**
                String edgeDescription = wordParts[2];
                int edgeDestination = Integer.parseInt(wordParts[3]);

                // Apply stemming
                stemmer.add(rawWord.toCharArray(), rawWord.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                words.add(stemmedWord);// that
                descriptions.add(edgeDescription);// compl
                destinations.add(edgeDestination);// 3
                wordNodesSize++;
            }

            for(int j = 0; j < wordNodesSize; j++) {//that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0
                // if the word is in the golden standard it's a lexeme <3
                if(goldenStandard.contains(words.get(j))) {
                    mapKey.set("+" + words.get(j));// +that
                    // count(l) for each word for itself.
                    context.write(mapKey, totalCount);// +that 3092
                }
                if(destinations.get(j) != 0 && MostCommonFeatures.containsKey(words.get(j))) {// if the word is a feature
                    String wordDesc =words.get(j) + "-" + descriptions.get(j);// that-compl
                    if(goldenStandard.contains(words.get(destinations.get(j) - 1))){// if the destination is a lexeme
                        //
                        mapKey.set(words.get(destinations.get(j) - 1) + "#" + wordDesc);// experience#that-compl
                        context.write(mapKey, totalCount);// experience#that-compl 3092
                        //**change I did (Niv 14.3)**
                        mapKey.set(wordDesc);// that-compl
                        context.write(mapKey, totalCount);// that-compl 3092
                    }
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text, DoubleWritable> {
        double L = 0;
        double F = 0;
        public HashMap<String, Double> hashMap = new HashMap<>(); // l, sum
        private MultipleOutputs<Text, DoubleWritable> multipleOutputs;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
            // Count L and F according to F.txt and L.txt which have only 1 line of integer each.
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            S3Object s3object = s3.getObject("nivolarule05032025", "L.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                L = Double.parseDouble(line);
            }
            S3Object s3object1 = s3.getObject("nivolarule05032025", "F.txt");
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(s3object1.getObjectContent()));
            String line1;
            while ((line1 = reader1.readLine()) != null) {
                F = Double.parseDouble(line1);
            }

        }

        @Override//
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // lexicographically * -> + -> else.

            double sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (key.charAt(0) == '+') { //p(l)
                double prob = sum / L;
                String lexema = key.toString().substring(1);
                multipleOutputs.write("lexemes", new Text(lexema), new DoubleWritable(prob), "lexemes/part");
                hashMap.put(lexema, sum);
                multipleOutputs.write("probabilities", new Text(lexema), new DoubleWritable(prob), "probabilities/part");

            } else {
                // experience#that-compl 3092
                if (key.find("#") != -1) { // p(f | l)
                    String l = key.toString().split("#")[0];
                    String f = key.toString().split("#")[1];

                    double probLf = sum / L; // p(l,f)
                    multipleOutputs.write("probabilities", new Text(l + "," + f), new DoubleWritable(probLf), "probabilities/part");
                    double probLGivenF = sum / hashMap.get(l); // p(f | l) = count(l,f) / count(L=l)
                    multipleOutputs.write("probabilities", new Text(f + "|" + l), new DoubleWritable(probLGivenF), "probabilities/part");
                } else { //p(f)
                    double prob = sum / F;
                    multipleOutputs.write("probabilities", key, new DoubleWritable(prob), "probabilities/part");// feature, prob
                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws
                IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if(key.find("#") != -1) { // +l, l#f we want +l and l#f to be together -> isolate +l in both
                String l = "+" + key.toString().split("#")[0];
                return Math.abs(l.hashCode()) % numPartitions;
            }

            return Math.abs(key.toString().hashCode()) % numPartitions;
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
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/output/lineOfLexemesInGS"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/probs"));
        MultipleOutputs.addNamedOutput(job, "lexemes", TextOutputFormat.class, Text.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "probabilities", TextOutputFormat.class, Text.class, DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

