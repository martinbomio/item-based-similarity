package edu.mapreduce.formats.job;

import edu.mapreduce.formats.UserRatingsSummary;

/**
 * Hadoop's serializable version of the user user rating  summary object using abstract ObjectWritable class.
 */
public class UserRatingsSummaryWritable extends ObjectWritable<UserRatingsSummary> {
    public UserRatingsSummaryWritable() {
        super();
    }

    public UserRatingsSummaryWritable(UserRatingsSummary object) {
        super(object);
    }
}
