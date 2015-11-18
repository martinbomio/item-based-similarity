package edu.mapreduce.similarity.job;

import edu.mapreduce.formats.job.ObjectWritable;
import edu.mapreduce.similarity.MovieRatingPair;

/**
 *
 *
 */
public class MovieRatingPairWritable extends ObjectWritable<MovieRatingPair> {
    public MovieRatingPairWritable() {
        super();
    }

    public MovieRatingPairWritable(MovieRatingPair object) {
        super(object);
    }
}
