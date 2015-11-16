package edu.mapreduce.histogram;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MovieRatingSummarySerializerTest {

    @Test
    public void testSerializer() {
        MovieRatingSummary summary = new MovieRatingSummary(1L);
        summary.incrementRatingCount("4");
        summary.incrementRatingCount("4");
        summary.incrementRatingCount("4");
        summary.incrementRatingCount("1");
        String serialized = MovieRatingSummarySerializer.serialize(summary);
        String expected = "1\u00011\u00021\u00014\u00023";
        assertEquals(expected, serialized);
    }


}
