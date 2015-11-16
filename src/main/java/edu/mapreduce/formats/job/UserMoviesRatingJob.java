package edu.mapreduce.formats.job;

import edu.mapreduce.formats.UserMovieRating;
import edu.mapreduce.formats.UserRatingsSummary;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Creates a summary of the movies that the user rated using custom input/output
 * formats.
 */
public class UserMoviesRatingJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new UserMoviesRatingJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "User Movie Ratings Summary Job");
        job.setJarByClass(UserMoviesRatingJob.class);

        // Setup MapReduce job
        job.setMapperClass(UserMovieRatingsMapper.class);
        job.setReducerClass(UserMovieRatingsReducer.class);

        // Specify key / value
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(UserMovieRatingWritable.class);
        job.setOutputValueClass(UserRatingsSummaryWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(UserMovieRatingInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(UserRatingsSummaryOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Aggregates the user move ratings per user.
     */
    public static class UserMovieRatingsMapper extends
            Mapper<LongWritable, UserMovieRatingWritable, LongWritable, UserMovieRatingWritable> {

        @Override
        protected void map(LongWritable key, UserMovieRatingWritable value, Context context)
                throws IOException, InterruptedException {
            UserMovieRating userMovieRating = value.get();
            context.write(new LongWritable(userMovieRating.getUserId()), value);
        }
    }

    /**
     * Creates the user rating summary
     */
    public static class UserMovieRatingsReducer
            extends
            Reducer<LongWritable, UserMovieRatingWritable, LongWritable, UserRatingsSummaryWritable> {

        @Override
        protected void reduce(LongWritable key,
                              Iterable<UserMovieRatingWritable> values,
                              Context context) throws IOException, InterruptedException {
            UserRatingsSummary summary = new UserRatingsSummary(key.get());
            for (UserMovieRatingWritable value : values) {
                UserMovieRating userMovieRating = value.get();
                summary.addRating(userMovieRating.getMovieId(), userMovieRating.getRating());
            }
            context.write(key, new UserRatingsSummaryWritable(summary));
        }
    }
}
