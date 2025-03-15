package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

//filtered_features
// (dog-subj 30, dog-obj 20, like-subj 10, like-obj 5...) 10 vectors of length 100

public class Step4 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text mapKey = new Text();
        double L = 0;
        int fi = 0;
        private final HashMap<String, Integer> fIndexes = new HashMap<String, Integer>();

        // l, p(l)
        private final HashMap<String, Double> ls = new HashMap<String, Double>();
        // <l, <f, <p(l,f), p(f|l)>>>
        private final HashMap<String, HashMap<String, Pair<Double, Double>>> lfs = new HashMap<>();


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            S3Object s3object = s3.getObject("nivolarule05032025", "L.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                L = Double.parseDouble(line);
            }
//            //todo: why can't findexes be in the reducer? why do we need it in the mpaper?
//            AmazonS3 s3_1 = AmazonS3ClientBuilder.defaultClient();
//            ListObjectsV2Result result = s3_1.listObjectsV2("nivolarule05032025", "filtered_features/");
//            List<S3ObjectSummary> objects = result.getObjectSummaries();
//            for (S3ObjectSummary os : objects) {
//                S3Object s3object_1 = s3_1.getObject("nivolarule05032025", os.getKey());
//                BufferedReader reader_1 = new BufferedReader(new InputStreamReader(s3object_1.getObjectContent()));
//                String line_1;
//                while ((line_1 = reader_1.readLine()) != null) {
//                    String[] parts = line_1.split("\t");
//                    if (parts.length != 2) return; // Skip malformed lines
//                    try {
//                        String feature = parts[0];
//                        fIndexes.put(feature, fIndexes.size());
//                    } catch (NumberFormatException e) {
//                        // Ignore lines where the count is not a valid integer
//                    }
//                }
//            }
            AmazonS3 s3_2 = AmazonS3ClientBuilder.defaultClient();
            ListObjectsV2Result result_2 = s3_2.listObjectsV2("nivolarule05032025", "probs/lexemes/");
            List<S3ObjectSummary> objects_2 = result_2.getObjectSummaries();
            for (S3ObjectSummary os : objects_2) {
                S3Object s3object_2 = s3_2.getObject("nivolarule05032025", os.getKey());
                BufferedReader reader_2 = new BufferedReader(new InputStreamReader(s3object_2.getObjectContent()));
                String line_2;
                while ((line_2 = reader_2.readLine()) != null) {
                    String[] parts = line_2.split("\t");
                    if (parts.length != 2) return; // Skip malformed lines
                    try {
                        String lexeme = parts[0];
                        double prob = Double.parseDouble(parts[1]);
                        ls.put(lexeme, prob);
                    } catch (NumberFormatException e) {
                        // Ignore lines where the count is not a valid integer
                    }
                }
            }
            System.out.println("[DEBUG] Loaded lexeme probabilities: " + ls.size());
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

            if (lORf.indexOf('|') != -1) { //p(f|l)
                String[] parts = lORf.split("\\|");//like|dog-subj
                String f = parts[0];
                String l = parts[1];

                mapKey.set(l);
                context.write(mapKey, new Text(f + "#p(f|l)#" + p));
            } else if (lORf.indexOf(',') != -1) { // p(l,f)
                String[] parts = lORf.split(",");
                String l = parts[0];
                String f = parts[1];

                mapKey.set(l);
                context.write(mapKey, new Text(f + "#p(l,f)#" + p));
            } else if (lORf.indexOf('-') != -1) { // dog-subj
                //for given f, for every l, send <l, f+"#p(f)#"+p(f)>
                for (String l : ls.keySet()) {
                    mapKey.set(l);
                    context.write(mapKey, new Text(lORf + "#p(f)#" + p));
//                    context.write(mapKey, new Text(lORf + "#Index#" + fIndexes.get(lORf)));// l f#Index#3
                }

            } else { // l, 0.45
                //for every f, send <l, f+"#p(l)#"+p(l)>
//                for (String f : fIndexes.keySet()) {
//                    mapKey.set(lORf);
//                    context.write(mapKey, new Text(f + "#p(l)#" + p));
//                }
                mapKey.set(lORf);
                context.write(mapKey, new Text(   "#p(l)#" + p));

                mapKey.set(lORf);
                context.write(mapKey, new Text("#L#" + L));
            }
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        //f , place in co-vector
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

            //todo: why can't findexes be in the reducer? why do we need it in the mpaper?
            // Load feature indexes
            AmazonS3 s3_1 = AmazonS3ClientBuilder.defaultClient();
            ListObjectsV2Result result = s3_1.listObjectsV2("nivolarule05032025", "filtered_features/");
            List<S3ObjectSummary> objects = result.getObjectSummaries();

            for (S3ObjectSummary os : objects) {
                S3Object s3object_1 = s3_1.getObject("nivolarule05032025", os.getKey());
                BufferedReader reader_1 = new BufferedReader(new InputStreamReader(s3object_1.getObjectContent()));
                String line_1;
                while ((line_1 = reader_1.readLine()) != null) {
                    String[] parts = line_1.split("\t");
                    if (parts.length == 2) {
                        fIndexes.put(parts[0], fIndexes.size());
                    }
                }
            }

            System.out.println("[DEBUG] Reducer fIndexes size: " + fIndexes.size());
        }

        @Override //l ["#L#586943", "f1#p(l)#p(l)","f2,p(l,f2)", "f1,p(f1)"..., "f1#Index#3"]
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //iterate through values
            //hashMap of: f, [p(l), p(f), p(l,f), p(f|l)]
            // for a specific l, for every f, collect all of its data to a hashmap
            double lsSize = 0;
            double pofl = 0.0;
            HashMap<String, double[]> fData = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split("#");
                String f = parts[0];//f1
                String wordOfFeature = f.split("-")[0];//dog
                String type = parts[1];
                double p = Double.parseDouble(parts[2]);

                // f#p(f|l)#p(f|l), f#p(l,f)#p(l,f), #p(l)#p(l), #L#586943, f#p(f)#p(f)
                if (type.equals("L")) {//#L#586943
                    lsSize = p;
                    continue;
                }

                if(type.equals("p(l)")) {//#p(l)#p(l)
                    pofl = p;
                    continue;
                }

                if(f.isEmpty()) {//
                    continue;
                }


                if (!fData.containsKey(wordOfFeature)) { //f p(..)
                    double[] probs = {0.0, 0.0, 0.0, 0.0};
                    fData.put(wordOfFeature, probs);
                }
//                if (type.equals("Index")) {
//                    if(!fIndexes.containsKey(f))
//                        fIndexes.put(f, (int) p);// f1, 3
//                } else {
                double[] arr = fData.get(wordOfFeature);
                arr[mapping.get(type)] = p;
//                fData.get(f)[mapping.get(type)] = p; //todo: maybe can be problematic - null and stuff
//                }


                //fData(f1) -> {0.2,0.3,0.4,0.5} -> [p(l), p(f), p(l,f), p(f|l)]
            }


            // then perform the calculations and emit the results
            int fsSize = fIndexes.size();
            Double[] v5 = new Double[fsSize];
            // (dog-subj,dog-obj,like-subj,like-obj...)
            Double[] v6 = new Double[fsSize];
            Double[] v7 = new Double[fsSize];
            Double[] v8 = new Double[fsSize];
            for(int i = 0; i < fsSize; i++) {
                v5[i] = 0.0;
                v6[i] = 0.0;
                v7[i] = 0.0;
                v8[i] = 0.0;
            }


            for (Map.Entry<String, double[]> fProbs : fData.entrySet()) {//
                String f = fProbs.getKey();
                double[] probs = fProbs.getValue();//[0.2,0.3,0.4,0.5]

                System.out.println("[DEBUG] Checking feature: " + f);
                System.out.println("[DEBUG] fIndexes contains " + f + "? " + fIndexes.containsKey(f));

                //
                System.out.println("[DEBUG] p(l) = " + pofl);
                System.out.println("[DEBUG] p(f) = " + probs[1]);
                System.out.println("[DEBUG] p(l,f) = " + probs[2]);
                System.out.println("[DEBUG] p(f|l) = " + probs[3]);
                //
                if(!fIndexes.containsKey(f)) {
                    System.out.println("[ERROR] Feature not found in fIndexes: " + f);
                    continue;
                }

//                double pl = probs[0]; //p(l)
                double pl = pofl; //p(l)
                double pf = probs[1]; //p(f)
                double plandf = probs[2]; //p(l,f)
                double pfGivenl = probs[3]; //p(f|l)


                //5: count(l,f) -> p(l,f) * L (because we get the probabilities, not counts)
                double lf5 = plandf * lsSize;
                //6: P(f|l)
                double lf6 = pfGivenl;// pfGivenl is zero, problem
                //7: log_2(p(l,f)/(p(l)*p(f)))
                double lf7 = 0.0;
                double lf8 = 0.0;

                if(pf != 0 && pl != 0) {
                    if(plandf != 0) {
                        lf7 = Math.log(plandf / (pl * pf)) / Math.log(2);
                    }
                    lf8 = (plandf - pl * pf) / Math.sqrt(pl * pf);
                }

//                double lf7 = pl;// check if pl is getting 0
                //8: (p(l,f) - p(l)*p(f))/(sqrt(p(l)*p(f)))

//                double lf8 = pf;// check if pf is getting 0
                int index = fIndexes.get(f);//3

                v5[index] = lf5;//plandf
                v6[index] = lf6;//pfGivenl
                v7[index] = lf7;//pl
                v8[index] = lf8;//pf
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
            return Math.abs(key.toString().hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/probs/probabilities"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/dataset"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}