package edu.mapreduce.histogram;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;

/**
 * Serializer for MovieRatingSummary object.
 */
public class MovieRatingSummarySerializer {
    public static final String FIRST_SEPARATOR = "\u0001";
    public static final String SECOND_SEPARATOR = "\u0002";
    public static final Joiner JOINER = Joiner.on(FIRST_SEPARATOR);

    public static String serialize(MovieRatingSummary movieRatingSummary) {
        Multiset<String> ratingCounts = movieRatingSummary.getRatingCounts();
        List<String> ratingCountString = Lists.newArrayList();
        for (String rating : ratingCounts) {
            ratingCountString.add(rating + SECOND_SEPARATOR + ratingCounts.count(rating));
        }
        return JOINER.join(ratingCounts);
    }
}
