package edu.mapreduce.formats;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

/**
 * Represents a line in the MoveLens user data file. Each object contains the
 * rating a user has given to a movie at a given time.
 */
public class MovieRating implements Serializable {
    private long userId;
    private long movieId;
    private int rating;
    private long timestamp;

    public MovieRating(String line) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MovieRating)) return false;

        MovieRating that = (MovieRating) o;

        if (movieId != that.movieId) return false;
        if (rating != that.rating) return false;
        if (timestamp != that.timestamp) return false;
        if (userId != that.userId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (userId ^ (userId >>> 32));
        result = 31 * result + (int) (movieId ^ (movieId >>> 32));
        result = 31 * result + rating;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
