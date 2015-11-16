package edu.mapreduce.histogram.job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CountMovieUsersRatingsJobTest {
    MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        CountMovieUsersRatingsJob.MovieRatingHistogramMapper mapper = new CountMovieUsersRatingsJob.MovieRatingHistogramMapper();
        CountMovieUsersRatingsJob.MovieRatingHistogramReducer reducer = new CountMovieUsersRatingsJob.MovieRatingHistogramReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.addAll(createMapInput("100\t1\t3\t881250949\n" +
                                        "101\t1\t2\t891717742\n" +
                                        "102\t2\t1\t878887116"));
        mapDriver.addOutput(new LongWritable(1), new Text("100\u00013"));
        mapDriver.addOutput(new LongWritable(1), new Text("101\u00012"));
        mapDriver.addOutput(new LongWritable(2), new Text("102\u00011"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.addAll(createReducerInput());
        reduceDriver.addOutput(new LongWritable(1), new Text("1\u00013\u00021\u00012\u00021"));
        reduceDriver.addOutput(new LongWritable(2), new Text("2\u00011\u00021"));
        reduceDriver.runTest();
    }

    @Test
    public void testJob() throws IOException {
        mapReduceDriver.addAll(createMapInput("100\t1\t3\t10001\n" +
                                              "101\t1\t3\t10002\n" +
                                              "102\t1\t2\t10003\n" +
                                              "103\t2\t5\t10004"));
        mapReduceDriver.addOutput(new LongWritable(1), new Text("1\u00013\u00022\u00012\u00021"));
        mapReduceDriver.addOutput(new LongWritable(2), new Text("2\u00015\u00021"));
        mapReduceDriver.runTest();
    }

    private List<Pair<LongWritable, List<Text>>> createReducerInput() {
        List<Pair<LongWritable, List<Text>>> inputs = Lists.newArrayList();
        inputs.add(new Pair<LongWritable, List<Text>>(new LongWritable(1), Arrays.asList(new Text("100\u00013"),
                                                                                         new Text("101\u00012"))));
        inputs.add(new Pair<LongWritable, List<Text>>(new LongWritable(2), Arrays.asList(new Text("102\u00011"))));
        return inputs;
    }

    private List<Pair<LongWritable, Text>> createMapInput(String input) {
        List<Pair<LongWritable, Text>> inputs = Lists.newArrayList();
        int i = 0;
        for (String line : StringUtils.split(input, "\n")) {
            inputs.add(new Pair<LongWritable, Text>(new LongWritable(i), new Text(line)));
        }
        return inputs;

    }

}
