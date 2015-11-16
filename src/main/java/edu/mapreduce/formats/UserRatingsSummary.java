package edu.mapreduce.formats;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Represents all the movies a user has rated.
 */
public class UserRatingsSummary implements Serializable {
    private long userId;
    private Map<Long, Long> moveRatings;

    public UserRatingsSummary() {
    }

    public UserRatingsSummary(long userId) {
        this.userId = userId;
        this.moveRatings = Maps.newHashMap();
    }

    public void addRating(long movieId, long rating) {
        this.moveRatings.put(movieId, rating);
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public Map<Long, Long> getMoveRatings() {
        return moveRatings;
    }

    public void setMoveRatings(Map<Long, Long> moveRatings) {
        this.moveRatings = moveRatings;
    }
}
