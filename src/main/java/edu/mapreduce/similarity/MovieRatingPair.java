package edu.mapreduce.similarity;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *
 *
 */
public class MovieRatingPair implements Serializable {
    private static final HashFunction HASHER = Hashing.md5();
    private static final Joiner JOINER = Joiner.on("-");
    private List<Long> movies;
    private List<Long> ratings;

    public MovieRatingPair() {
    }

    public MovieRatingPair(List<Long> movies, List<Long> ratings) {
        this.movies = movies;
        this.ratings = ratings;
    }

    public List<Long> getMovies() {
        return movies;
    }

    public void setMovies(List<Long> movies) {
        this.movies = movies;
    }

    public List<Long> getRatings() {
        return ratings;
    }

    public void setRatings(List<Long> ratings) {
        this.ratings = ratings;
    }

    public int hash() {
        return HASHER.hashString(JOINER.join(movies)).asInt();
    }
}
