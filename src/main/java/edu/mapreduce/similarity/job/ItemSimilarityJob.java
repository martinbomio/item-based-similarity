package edu.mapreduce.similarity.job;

import edu.mapreduce.formats.UserRatingsSummary;
import edu.mapreduce.similarity.MovieRatingPair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math.stat.correlation.PearsonsCorrelation;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 *
 *
 */
public class ItemSimilarityJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ItemSimilarityJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Item Similarity Job");
        job.setJarByClass(ItemSimilarityJob.class);

        // Setup MapReduce job
        job.setMapperClass(CooccurancesMapper.class);
        job.setReducerClass(SimilarityReducer.class);

        // Specify key / value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MovieRatingPairWritable.class);
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

    public static class CooccurancesMapper extends
            Mapper<LongWritable, Text, IntWritable, MovieRatingPairWritable> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private static final Comparator<Entry<Long, Long>> BY_MOVIE = new Comparator<Entry<Long, Long>>() {
            @Override
            public int compare(Entry<Long, Long> one, Entry<Long, Long> other) {
                return Longs.compare(one.getKey(), other.getKey());
            }
        };

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            UserRatingsSummary userRatingsSummary = objectMapper.readValue(value.toString(), UserRatingsSummary.class);
            Set<Entry<Long, Long>> entries = userRatingsSummary.getMoveRatings().entrySet();
            Entry<Long, Long>[] occurances = entries.toArray(new Entry[entries.size()]);
            Arrays.sort(occurances, BY_MOVIE);
            for (int i = 0; i < occurances.length; i++) {
                Entry<Long, Long> oneOccurance = occurances[i];
                for (int j = i; j < occurances.length; j++) {
                    Entry<Long, Long> otherOccurance = occurances[j];
                    MovieRatingPair movieRatingPair = new MovieRatingPair(Arrays.asList(oneOccurance.getKey(),
                                                                                        otherOccurance.getKey()),
                                                                          Arrays.asList(oneOccurance.getValue(),
                                                                                        otherOccurance.getValue()));
                    context.write(new IntWritable(movieRatingPair.hash()),
                                  new MovieRatingPairWritable(movieRatingPair));

                }
            }
        }
    }

    public static class SimilarityReducer extends
            Reducer<IntWritable, MovieRatingPairWritable, NullWritable, Text> {
        private static final Joiner JOINER = Joiner.on(",");

        @Override
        protected void reduce(IntWritable key,
                              Iterable<MovieRatingPairWritable> values,
                              Context context) throws IOException, InterruptedException {
            List<Long> oneDist = Lists.newArrayList();
            List<Long> otherDist = Lists.newArrayList();
            List<Long> movies = null;
            for (MovieRatingPairWritable value : values) {
                MovieRatingPair movieRatingPair = value.get();
                movies = movieRatingPair.getMovies();
                List<Long> ratings = movieRatingPair.getRatings();
                oneDist.add(ratings.get(0));
                otherDist.add(ratings.get(1));
            }
            if (oneDist.size() != otherDist.size()) {
                throw new RuntimeException("Distribution sized don't match");
            }
            double similarity = 0;
            if (movies.get(0).equals(movies.get(1))) {
                similarity = 1;
            } else if (oneDist.size() != 1) {
                similarity = new PearsonsCorrelation().correlation(toArray(oneDist), toArray(otherDist));
            }
            context.write(NullWritable.get(), new Text(JOINER.join(movies.get(0), movies.get(1), similarity)));
        }

        private double[] toArray(List<Long> oneDist) {
            double[] dist = new double[oneDist.size()];
            for (int i = 0; i < oneDist.size(); i++) {
                dist[i] = oneDist.get(i);
            }
            return dist;
        }
    }
}
