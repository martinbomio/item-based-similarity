package edu.mapreduce.formats.job;

import edu.mapreduce.formats.MovieRating;
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
 *
 * Input: (tab separated)
 *     userID    movieID    rating    timestamp
 *
 * Example:
 *       1000       10         4       10000000
 *       1010       21         3       10120120
 *       1000       21         1       21312310
 *
 * Output:
 *    A json object for each user containing all his/her the movie ratings.
 *
 * Example:
 *       {"userId": 1000, "movieRatings": {"10": 4, "21":1}}
 *       {"userId": 1010, "movieRatings": {"21":3}}
 *
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
        job.setMapOutputValueClass(MovieRatingWritable.class);
        job.setOutputValueClass(UserRatingsSummaryWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(MovieRatingInputFormat.class);

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
            Mapper<LongWritable, MovieRatingWritable, LongWritable, MovieRatingWritable> {

        @Override
        protected void map(LongWritable key, MovieRatingWritable value, Context context)
                throws IOException, InterruptedException {
            MovieRating movieRating = value.get();
            context.write(new LongWritable(movieRating.getUserId()), value);
        }
    }

    /**
     * Creates the user rating summary
     */
    public static class UserMovieRatingsReducer
            extends
            Reducer<LongWritable, MovieRatingWritable, LongWritable, UserRatingsSummaryWritable> {

        @Override
        protected void reduce(LongWritable key,
                              Iterable<MovieRatingWritable> values,
                              Context context) throws IOException, InterruptedException {
            UserRatingsSummary summary = new UserRatingsSummary(key.get());
            for (MovieRatingWritable value : values) {
                MovieRating movieRating = value.get();
                summary.addRating(movieRating.getMovieId(), movieRating.getRating());
            }
            context.write(key, new UserRatingsSummaryWritable(summary));
        }
    }
}
