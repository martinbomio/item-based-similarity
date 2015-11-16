package edu.mapreduce.formats;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

/**
 * Represents a line in the MoveLens user data file. Each object contains the
 * rating a user has given to a movie at a given time.
 */
public class UserMovieRating implements Serializable {
    private long userId;
    private long movieId;
    private int rating;
    private long timestamp;

    public UserMovieRating(String line) {
        String[] split = StringUtils.split(line, "\t");
        userId = Long.valueOf(split[0]);
        movieId = Long.valueOf(split[1]);
        rating = Integer.valueOf(split[2]);
        timestamp = Long.valueOf(split[3]);
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getMovieId() {
        return movieId;
    }

    public void setMovieId(long movieId) {
        this.movieId = movieId;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}