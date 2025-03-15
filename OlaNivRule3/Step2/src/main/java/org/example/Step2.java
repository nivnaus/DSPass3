package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return; // Skip malformed lines

            try {
                String feature = parts[0];
                int count = Integer.parseInt(parts[1]);

                // Emit count as negative to allow sorting in descending order
                // 1 2 3 4... 10000
                // ... -10000, -9999, -9998, ... -1
                context.write(new IntWritable(-count), new Text(feature));
            } catch (NumberFormatException e) {
                // Ignore lines where the count is not a valid integer
            }
        }
    }

    public static class ReducerClass extends Reducer<IntWritable, Text, Text, IntWritable> {
        private final PriorityQueue<FeatureEntry> topFeatures = new PriorityQueue<>();
        private static final int TOP_N = 100;// top 100 of one reducer - 1000 in total

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text feature : values) {
                topFeatures.add(new FeatureEntry(feature.toString(), -key.get())); // Convert back to positive count
                if (topFeatures.size() > TOP_N) {
                    topFeatures.poll(); // Remove the smallest if over limit
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!topFeatures.isEmpty()) {
                FeatureEntry entry = topFeatures.poll();
                context.write(new Text(entry.feature), new IntWritable(entry.count));
            }
        }

        private static class FeatureEntry implements Comparable<FeatureEntry> {
            String feature;
            int count;

            FeatureEntry(String feature, int count) {
                this.feature = feature;
                this.count = count;
            }

            @Override
            public int compareTo(FeatureEntry other) {
                return Integer.compare(this.count, other.count); // Ascending order for min-heap
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Match multiple feature files (e.g., features-r-00000, features-r-00001, ...)
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule05032025/output/features"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule05032025/filtered_features"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
