package org.example;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mapKey = new Text();
        private HashSet<String> stopWords = new HashSet<>(Arrays.asList(
                "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד",
                "מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו",
                "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש",
                "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא",
                "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
                "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו",
                "בה", "בא", "את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין",
                "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1",
                ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול",
                "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל",
                "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית",
                "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם",
                "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים",
                "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו",
                "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה",
                "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או",
                "אבל", "א"
        ));

        @Override//3gram: w1 w2 w3\tyear\tmatch_count\tpage_count\tvolume_count
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // parse 3gram input line
            String[] tabParse = value.toString().split("\t");
            String trio = tabParse[0];
            String trioMatchCount = tabParse[2];
            IntWritable trioMatchCountInt = new IntWritable(Integer.parseInt(trioMatchCount));
            String[] words = trio.split(" ");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            // extract the 5 subs -> filtering stop words
            if(!(stopWords.contains(w1) || stopWords.contains(w2)  || stopWords.contains(w3))) {
                // emit the 5 subs
                mapKey.set(w1+"#"+w2+"#"+w3);
                context.write(mapKey, trioMatchCountInt); //N3
                mapKey.set("*#"+w2+"#"+w3);
                context.write(mapKey, one); // N2
                mapKey.set("*#"+w2+"#*");
                context.write(mapKey, one); // C1
                mapKey.set("*#*#"+w3);
                context.write(mapKey, one); // N1
                mapKey.set(w1 + "#" + w2 + "#*");
                context.write(mapKey, one); //C2
                mapKey.set(w1 + "#*#*");
                context.write(mapKey, one); //for C0 in step 2
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
    // TODO: we need to change it for our assignment
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
        job.setCombinerClass(Step1.ReducerClass.class);
        job.setReducerClass(Step1.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
        // todo: start with a smaller file
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextInputFormat.addInputPath(job, new Path("s3://nivolarule29122024/exampleOf3gram.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://nivolarule29122024/subSums.txt"));// TODO: change this to our own bucket
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
