package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import javafx.util.Pair;
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

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
//        private final Text mapKey = new Text();
//        // f, p(f)
//        private final HashMap<String, Double> fs = new HashMap<String, Double>();
//
//        //f , place in co-vector
//
//        // l, p(l)
//        private final HashMap<String, Double> ls = new HashMap<String, Double>();
//        // <l, <f, <p(l,f), p(l|f)>>>
//        // dog, <like, <0.34, 0.0>>
//        private final HashMap<String, HashMap<String, Pair<Double, Double>>> lfs = new HashMap<>();


        @Override // like    0.43   OR    like,dog-subj     0.843   OR   dog-subj   0.31   OR   like|dog-subj   0.95
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, one);
//            String[] rowParts = value.toString().split("\t");
//            String lORf = rowParts[0]; // l or f ?
//            Double p = Double.parseDouble(rowParts[1]);
//
//            if(lORf.indexOf('|') != -1) { //p(l|f)
//                String[] parts = lORf.split("\\|");
//                String l = parts[0];
//                String f = parts[1];
//                if(lfs.containsKey(l)) {
//                    //all fs that were near l
//                    HashMap<String, Pair<Double, Double>> fsHash = lfs.get(l);
//                    if(fsHash.containsKey(f)) {
//                        Pair<Double, Double> pLF = fsHash.get(f);
//                        pLF = new Pair<Double, Double>(pLF.getKey(), p);
//                        fsHash.put(f, pLF);
//                    } else {
//                        fsHash.put(f, new Pair<Double, Double>(0.0, p));
//                    }
//                } else {
//                    HashMap<String, Pair<Double, Double>> fsHash = new HashMap<>();
//                    fsHash.put(f, new Pair<>(0.0, p));
//                    lfs.put(l, fsHash);
//                }
//            } else if(lORf.indexOf(',') != -1) { // p(l,f)
//                String[] parts = lORf.split(",");
//                String l = parts[0];
//                String f = parts[1];
//                if(lfs.containsKey(l)) {
//                    //all fs that were near l
//                    HashMap<String, Pair<Double, Double>> fsHash = lfs.get(l);
//                    if(fsHash.containsKey(f)) {
//                        Pair<Double, Double> pLF = fsHash.get(f);
//                        pLF = new Pair<Double, Double>(p, pLF.getValue());
//                        fsHash.put(f, pLF);
//                    } else {
//                        fsHash.put(f, new Pair<Double, Double>(p, 0.0));
//                    }
//                } else {
//                    HashMap<String, Pair<Double, Double>> fsHash = new HashMap<>();
//                    fsHash.put(f, new Pair<>(p, 0.0));
//                    lfs.put(l, fsHash);
//                }
//            } else if(lORf.indexOf('-') != -1) { // p(f)
//                fs.put(lORf, p);
//            } else { // p(l)
//                ls.put(lORf, p);
//            }
        }

        @Override
        public void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            for(String l : ls.keySet()) {
//                for(String f : fs.keySet()) {
//                    // if l * f doesn't exist in lfs, add l * f with 0s
//                    if(!lfs.containsKey(l) || !lfs.get(l).containsKey(f)) {
//                        if(!lfs.containsKey(l)) {
//                            lfs.put(l, new HashMap<>());
//                        }
//                        lfs.get(l).put(f, new Pair<>(0.0, 0.0));
//                    }
//                }
//            }
//            // <l, <f, <p(l,f), p(l|f)>>>
//            for(Map.Entry<String, HashMap<String, Pair<Double, Double>>> lHash: lfs.entrySet()) {
//                String l = lHash.getKey();
//                HashMap<String, Pair<Double, Double>> lh = lHash.getValue();
//
//                for(Map.Entry<String, Pair<Double, Double>> fProbs : lh.entrySet()) {
//                    String f = fProbs.getKey();
//                    Pair<Double, Double> prob = fProbs.getValue();
//
//                    Text lkey = new Text(l);
//
//                    // <l, "f,p(l),p(f),p(l,f),p(l|f)">
//                    context.write(lkey, new Text(f + "," + ls.get(l) + "," + fs.get(f) + "," + lfs.get(l).get(f).getKey() + "," + lfs.get(l).get(f).getValue()));
//                }
//            }
//        }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
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


        @Override // l ["f1,p(l),p(f1),p(l,f1),p(l|f1)", "f2,p(l),p(f2),p(l,f2),p(l|f2)", ... ]
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            Text value = key; // redundant, from mapper.

            // problem: l1 may get [f1, f2] l2 may get [f2, f1].  in the output vector, we need a unified format
            String[] rowParts = value.toString().split("\t");
            String lORf = rowParts[0]; // l or f ?
            Double p = Double.parseDouble(rowParts[1]);

            if(lORf.indexOf('|') != -1) { //p(l|f)
                String[] parts = lORf.split("\\|");
                String l = parts[0];
                String f = parts[1];
                if(lfs.containsKey(l)) {
                    //all fs that were near l
                    HashMap<String, Pair<Double, Double>> fsHash = lfs.get(l);
                    if(fsHash.containsKey(f)) {
                        Pair<Double, Double> pLF = fsHash.get(f);
                        pLF = new Pair<Double, Double>(pLF.getKey(), p);
                        fsHash.put(f, pLF);
                    } else {
                        fsHash.put(f, new Pair<Double, Double>(0.0, p));
                    }
                } else {
                    HashMap<String, Pair<Double, Double>> fsHash = new HashMap<>();
                    fsHash.put(f, new Pair<>(0.0, p));
                    lfs.put(l, fsHash);
                }
            } else if(lORf.indexOf(',') != -1) { // p(l,f)
                String[] parts = lORf.split(",");
                String l = parts[0];
                String f = parts[1];
                if(lfs.containsKey(l)) {
                    //all fs that were near l
                    HashMap<String, Pair<Double, Double>> fsHash = lfs.get(l);
                    if(fsHash.containsKey(f)) {
                        Pair<Double, Double> pLF = fsHash.get(f);
                        pLF = new Pair<Double, Double>(p, pLF.getValue());
                        fsHash.put(f, pLF);
                    } else {
                        fsHash.put(f, new Pair<Double, Double>(p, 0.0));
                    }
                } else {
                    HashMap<String, Pair<Double, Double>> fsHash = new HashMap<>();
                    fsHash.put(f, new Pair<>(p, 0.0));
                    lfs.put(l, fsHash);
                }
            } else if(lORf.indexOf('-') != -1) { // p(f)
                fs.put(lORf, p);
                fIndexes.put(lORf, fi);
                fi++;
            } else { // p(l)
                ls.put(lORf, p);
            }

        }

        @Override
        public void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            for(String l : ls.keySet()) {
                for(String f : fs.keySet()) {
                    // if l * f doesn't exist in lfs, add l * f with 0s
                    if(!lfs.containsKey(l) || !lfs.get(l).containsKey(f)) {
                        if(!lfs.containsKey(l)) {
                            lfs.put(l, new HashMap<>());
                        }
                        lfs.get(l).put(f, new Pair<>(0.0, 0.0));
                    }
                }
            }

            int fsSize = fIndexes.size();
            int lsSize = ls.size();

            // <l, <f, <p(l,f), p(f|l)>>>
            for(Map.Entry<String, HashMap<String, Pair<Double, Double>>> lHash: lfs.entrySet()) {
                String l = lHash.getKey();
                HashMap<String, Pair<Double, Double>> lh = lHash.getValue();

                Double[] v5 = new Double[fsSize];
                Double[] v6 = new Double[fsSize];
                Double[] v7 = new Double[fsSize];
                Double[] v8 = new Double[fsSize];

                for(Map.Entry<String, Pair<Double, Double>> fProbs : lh.entrySet()) {
                    String f = fProbs.getKey();
                    Pair<Double, Double> prob = fProbs.getValue();

                    Text lkey = new Text(l);

                    Double pl = ls.get(l); //p(l)
                    Double pf = fs.get(f); //p(f)
                    Double plandf = lfs.get(l).get(f).getKey(); //p(l,f)
                    Double pfGivenl = lfs.get(l).get(f).getValue(); //p(f|l)

                    //5: count(l,f) -> p(l,f) * L (because we get the probabilities, not counts)
                    Double lf5 = plandf * lsSize;
                    //6: P(f|l)
                    Double lf6 = pfGivenl;
                    //7: log_2(p(l,f)/(p(l)*p(f)))
                    Double lf7 = Math.log(plandf / (pl * pf)) / Math.log(2);
                    //8: (p(l,f) - p(l)*p(f))/(sqrt(p(l)*p(f)))
                    Double lf8 = (plandf - pl * pf) / Math.sqrt(pl * pf);

                    int index = fIndexes.get(f);

                    v5[index] = lf5;
                    v6[index] = lf6;
                    v7[index] = lf7;
                    v8[index] = lf8;
                }

                //format is: l  5   [0.32, 0.52, 0.456, 0.65]
                context.write(new Text(l + "\t5"), new Text(Arrays.toString(v5)));
                context.write(new Text(l + "\t6"), new Text(Arrays.toString(v6)));
                context.write(new Text(l + "\t7"), new Text(Arrays.toString(v7)));
                context.write(new Text(l + "\t8"), new Text(Arrays.toString(v8)));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return 0; // we want everything to go to the same reducer boy
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
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/probs.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/landf.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
