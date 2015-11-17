package edu.mapreduce.formats.job;

import edu.mapreduce.formats.UserRatingsSummary;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class UserMoviesRatingJobTest {
    @Rule
    public TemporaryPath tmpDir = new TemporaryPath();
    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testJob() throws Exception {
        Path inputPath = createInputs("100\t1\t3\t881250949\n" +
                                      "100\t2\t5\t881250949\n" +
                                      "101\t1\t2\t891717742\n" +
                                      "102\t2\t1\t878887116");
        Path output = tmpDir.getPath("output");
        UserMoviesRatingJob job = new UserMoviesRatingJob();
        job.setConf(tmpDir.getDefaultConfiguration());
        job.run(new String[] { inputPath.toString(), output.toString() });

        File file = tmpDir.getFile("output/part-r-00000");
        List<UserRatingsSummary> summaryList = getSummariesFromFile(file);

        UserRatingsSummary user100 = new UserRatingsSummary(100);
        user100.addRating(1, 3);
        user100.addRating(2, 5);
        Assert.assertEquals(user100, summaryList.get(0));

        UserRatingsSummary user101 = new UserRatingsSummary(101);
        user101.addRating(1, 2);
        Assert.assertEquals(user101, summaryList.get(1));

        UserRatingsSummary user102 = new UserRatingsSummary(102);
        user102.addRating(2, 1);
        Assert.assertEquals(user102, summaryList.get(2));
    }

    private List<UserRatingsSummary> getSummariesFromFile(File outputFile) throws IOException {
        List<UserRatingsSummary> summaries = Lists.newArrayList();
        for (String line : FileUtils.readLines(outputFile)) {
            summaries.add(objectMapper.readValue(line, UserRatingsSummary.class));
        }
        return summaries;
    }

    private Path createInputs(String input) throws IOException {
        Configuration conf = tmpDir.getDefaultConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path inputs = new Path(tmpDir.getRootPath(), "inputs");
        fs.mkdirs(inputs);
        FSDataOutputStream outputStream = fs.create(new Path(inputs, "input001"));
        IOUtils.write(input, outputStream);
        outputStream.close();
        return inputs;
    }

}
