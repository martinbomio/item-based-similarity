package edu.mapreduce.formats.job;

import edu.mapreduce.formats.MovieRating;

/**
 * Hadoop's serializable version of the user move rating object.
 */
public class MovieRatingWritable extends ObjectWritable<MovieRating> {
    public MovieRatingWritable() {
        super();
    }

    public MovieRatingWritable(MovieRating object) {
        super(object);
    }
}
