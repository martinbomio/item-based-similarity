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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserRatingsSummary)) return false;

        UserRatingsSummary that = (UserRatingsSummary) o;

        if (userId != that.userId) return false;
        if (moveRatings != null ? !moveRatings.equals(that.moveRatings) : that.moveRatings != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (userId ^ (userId >>> 32));
        result = 31 * result + (moveRatings != null ? moveRatings.hashCode() : 0);
        return result;
    }
}
