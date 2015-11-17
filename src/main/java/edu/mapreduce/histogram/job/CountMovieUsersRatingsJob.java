package edu.mapreduce.histogram.job;


import edu.mapreduce.histogram.MovieRatingSummary;
import edu.mapreduce.histogram.MovieRatingSummarySerializer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

/**
 * Simple hadoop job for creating movie rating histogram.
 * Input: (tab separated)
 *     userID    movieID    rating    timestamp
 *
 * Example:
 *       1000       10         4       10000000
 *       1010       21         3       10120120
 *       1000       21         1       21312310
 *
 * Output:
 *      movieID\u0001rating1\u0002ratingCount1\u0001rating2\u0002ratingCount2
 *
 * Example:
 *      10\u00014\u00021
 *      21\u00013\u00021\u00011\u00021
 *
 */
public class CountMovieUsersRatingsJob extends Configured implements Tool {
    private static final String MAPPER_SEPARATOR = "\u0001";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountMovieUsersRatingsJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Count Movie Ratings Job");
        job.setJarByClass(CountMovieUsersRatingsJob.class);

        // Setup MapReduce job
        job.setMapperClass(MovieRatingHistogramMapper.class);
        job.setReducerClass(MovieRatingHistogramReducer.class);

        // Specify key / value
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
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

    /**
     * Mapper class that aggregates user ratings per movie.
     */
    public static class MovieRatingHistogramMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private static final int USER_ID_INDEX = 0;
        private static final int ITEM_ID_INDEX = 1;
        private static final int RATING_INDEX = 2;
        private static final int TIMESTAMP_INDEX = 3;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] values = value.toString().split("\t");
            context.write(new LongWritable(Long.parseLong(values[ITEM_ID_INDEX])),
                          new Text(values[USER_ID_INDEX] + MAPPER_SEPARATOR + values[RATING_INDEX]));
        }

    }

    /**
     * Creates the histogram for each movie.
     */
    public static class MovieRatingHistogramReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private static final int USER_ID_INDEX = 0;
        private static final int RATING_INDEX = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            MovieRatingSummary movieRatingsSummary = new MovieRatingSummary(key.get());
            for (Text value : values) {
                String[] userRatings = value.toString().split(MAPPER_SEPARATOR);
                movieRatingsSummary.incrementRatingCount(userRatings[RATING_INDEX]);
            }
            context.write(key, new Text(MovieRatingSummarySerializer.serialize(movieRatingsSummary)));
        }
    }


}
