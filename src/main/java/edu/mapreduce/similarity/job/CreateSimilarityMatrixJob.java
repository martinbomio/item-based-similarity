package edu.mapreduce.similarity.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;

/**
 *
 *
 */
public class CreateSimilarityMatrixJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ItemSimilarityJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Create Similarity Matrix Job");
        job.setJarByClass(CreateSimilarityMatrixJob.class);

        // Setup MapReduce job
        job.setMapperClass(CreateSimilarityMatrixMapper.class);
        job.setReducerClass(CreateSimilarityMatrixReducer.class);

        // Specify key / value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CreateSimilarityMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] values = value.toString().split(",");
            context.write(new IntWritable(Integer.valueOf(values[0])), new Text(values[1] + "," + values[2]));
            if (!values[0].equals(values[1])) {
                context.write(new IntWritable(Integer.valueOf(values[1])), new Text(values[0] + "," + values[2]));
            }
        }
    }

    public static class CreateSimilarityMatrixReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private static final Joiner JOINER = Joiner.on("\t");

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text(key.get() + "\t" + JOINER.join(values)));
        }
    }
}
