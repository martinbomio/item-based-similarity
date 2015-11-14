package edu.mapreduce.histogram;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Representation of the movie rating histogram using a guava Multiset.
 */
public class MovieRatingSummary {
    private long itemID;
    private Multiset<String> ratingCounts;

    public MovieRatingSummary() {
    }

    public MovieRatingSummary(long itemID) {
        this.itemID = itemID;
        ratingCounts = HashMultiset.create();
    }

    public void incrementRatingCount(String rating) {
        ratingCounts.add(rating);
    }

    public void merge(Multiset<String> other) {
        for (String rating : other.elementSet()) {
            ratingCounts.setCount(rating, ratingCounts.count(rating) + other.count(rating));
        }
    }

    public long getItemID() {
        return itemID;
    }

    public void setItemID(long itemID) {
        this.itemID = itemID;
    }

    public Multiset<String> getRatingCounts() {
        return ratingCounts;
    }

    public void setRatingCounts(Multiset<String> ratingCounts) {
        this.ratingCounts = ratingCounts;
    }

}
