package edu.mapreduce.formats.job;

import edu.mapreduce.formats.UserMovieRating;

/**
 * Hadoop's serializable version of the user move rating object.
 */
public class UserMovieRatingWritable extends ObjectWritable<UserMovieRating> {
    public UserMovieRatingWritable() {
        super();
    }

    public UserMovieRatingWritable(UserMovieRating object) {
        super(object);
    }
}
