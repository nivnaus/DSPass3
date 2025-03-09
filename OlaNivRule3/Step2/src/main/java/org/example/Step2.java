package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.classification.InterfaceAudience;
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
import org.apache.hadoop.fs.FileSystem;
import org.example.Pair;


import javax.naming.Context;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text mapKey = new Text();
        // f, p(f)
        private final HashMap<String, Double> fs = new HashMap<String, Double>();

        //f , place in co-vector
        int fi = 0;
        private final HashMap<String, Integer> fIndexes = new HashMap<String, Integer>();

        // l, p(l)
        private final HashMap<String, Double> ls = new HashMap<String, Double>();
        // <l, <f, <p(l,f), p(f|l)>>>
        private final HashMap<String, HashMap<String, Pair<Double, Double>>> lfs = new HashMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // Create S3 client (AWS SDK will automatically pick up credentials)
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1")  // Specify the region your bucket is in
                    .build();
            // Specify your S3 bucket and object key
            String bucketName = "nivolarule05032025";
            String dirPath = "output/output2/"; // add 00001, 00002...

//            // Read file from S3

            ListObjectsV2Result result = s3Client.listObjectsV2(bucketName, dirPath);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                try (S3Object s3Object = s3Client.getObject(bucketName, summary.getKey());
                     BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String word = line.split("\t")[0];
                        if (word.contains("-")) {
                            fs.put(word, 0.0);
                            fIndexes.put(word, fIndexes.size());
                        } else {
                            ls.put(word, 0.0);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file " + summary.getKey() + ": " + e.getMessage());
                }
            }
        }

        @Override // like    0.43   OR    like,dog-subj     0.843   OR   dog-subj   0.31   OR   like|dog-subj   0.95
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // mapper: list of L x F
            // send l "f,p(l)" for every f, l "f,p(f)" for every f,etc.
            // map input: l p(l) -> l "f,p(l)" for every f
            // map input: f p(f) -> l "f,p(f)" for every l
            // map input: l,f p(l,f) -> l "f,p(l,f)"


            // problem: l1 may get [f1, f2] l2 may get [f2, f1].  in the output vector, we need a unified format
            String[] rowParts = value.toString().split("\t");
            String lORf = rowParts[0]; // l or f ?
            double p = Double.parseDouble(rowParts[1]);

            if (lORf.indexOf('|') != -1) { //p(l|f)
                String[] parts = lORf.split("\\|");
                String l = parts[0];
                String f = parts[1];
                mapKey.set(l);
                context.write(mapKey, new Text(f + "#p(f|l)#" + p));
            } else if (lORf.indexOf(',') != -1) { // p(l,f)
                String[] parts = lORf.split(",");
                String l = parts[0];
                String f = parts[1];
                mapKey.set(l);
                context.write(mapKey, new Text(f + "#p(l,f)#" + p));
            } else if (lORf.indexOf('-') != -1) { // p(f)
                //for given f, for every l, send <l, f+"#p(f)#"+p(f)>
                for (String l : ls.keySet()) {
                    mapKey.set(l);
                    context.write(mapKey, new Text(lORf + "#p(f)#" + p));
                    context.write(mapKey, new Text(lORf + "#Index#" + fIndexes.get(lORf)));
                }

            } else { // p(l)
                //for every f, send <l, f+"#p(l)#"+p(l)>
                for (String f : fs.keySet()) {
                    mapKey.set(lORf);
                    context.write(mapKey, new Text(f + "#p(l)#" + p));
                }
                mapKey.set(lORf);
                context.write(mapKey, new Text("#L#" + ls.size()));
            }
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private final Text mapKey = new Text();
        // f, p(f)
//        private final HashMap<String, Double> fs = new HashMap<String, Double>();

        //f , place in co-vector
//        int fi = 0;
        private final HashMap<String, Integer> fIndexes = new HashMap<String, Integer>();

        // l, p(l)
//        private final HashMap<String, Double> ls = new HashMap<String, Double>();
        // <l, <f, <p(l,f), p(f|l)>>>
//        private final HashMap<String, HashMap<String, Pair<Double, Double>>> lfs = new HashMap<>();

        HashMap<String, Integer> mapping = new HashMap<>(); //mapping of p(..) to index

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            mapping.put("p(l)", 0);
            mapping.put("p(f)", 1);
            mapping.put("p(l,f)", 2);
            mapping.put("p(f|l)", 3);
        }

        @Override //l ["#L#586943", "f1#p(l)#p(l)","f2,p(l,f2)", "f1,p(f1)"...]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //iterate through values
            //hashMap of: f, [p(l), p(f), p(l,f), p(f|l)]
            // for a specific l, for every f, collect all of its data to a hashmap
            double lsSize = 0;
            HashMap<String, double[]> fData = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split("#");
                String f = parts[0];
                String type = parts[1];
                double p = Double.parseDouble(parts[2]);
                if (type.equals("L")) {
                    lsSize = p;
                } else if (type.equals("Index")) {
                    fIndexes.put(f, (int) p);
                }
                if (!fData.containsKey(f)) {
                    double[] probs = {0.0, 0.0, 0.0, 0.0};
                    fData.put(f, probs);
                }
                fData.get(f)[mapping.get(type)] = p; //todo: i hope it returns a ref like a normal language
            }
            // then perform the calculations and emit the results
            int fsSize = fData.size();
            // for every f, for every l,
            Double[] v5 = new Double[fsSize];
            Double[] v6 = new Double[fsSize];
            Double[] v7 = new Double[fsSize];
            Double[] v8 = new Double[fsSize];

            for (Map.Entry<String, double[]> fProbs : fData.entrySet()) {
                String f = fProbs.getKey();
                double[] probs = fProbs.getValue();

//                Text lkey = new Text(key.toString());

                double pl = probs[0]; //p(l)
                double pf = probs[1]; //p(f)
                double plandf = probs[2]; //p(l,f)
                double pfGivenl = probs[3]; //p(f|l)

                //5: count(l,f) -> p(l,f) * L (because we get the probabilities, not counts)
                double lf5 = plandf * lsSize;
                //6: P(f|l)
                double lf6 = pfGivenl;
                //7: log_2(p(l,f)/(p(l)*p(f)))
                double lf7 = Math.log(plandf / (pl * pf)) / Math.log(2);
                //8: (p(l,f) - p(l)*p(f))/(sqrt(p(l)*p(f)))
                double lf8 = (plandf - pl * pf) / Math.sqrt(pl * pf);

                int index = fIndexes.get(f);

                v5[index] = lf5;
                v6[index] = lf6;
                v7[index] = lf7;
                v8[index] = lf8;
            }

            //format is: l  5   [0.32, 0.52, 0.456, 0.65]
            context.write(new Text(key.toString() + "\t5"), new Text(Arrays.toString(v5)));
            context.write(new Text(key.toString() + "\t6"), new Text(Arrays.toString(v6)));
            context.write(new Text(key.toString() + "\t7"), new Text(Arrays.toString(v7)));
            context.write(new Text(key.toString() + "\t8"), new Text(Arrays.toString(v8)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % 7;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/output/output1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/landf.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
