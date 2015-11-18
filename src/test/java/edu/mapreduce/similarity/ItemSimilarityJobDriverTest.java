package edu.mapreduce.similarity;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class ItemSimilarityJobDriverTest {

    @Rule
    public TemporaryPath tmpDir = new TemporaryPath();

    @Test
    public void testJob() throws Exception {
        Path inputPath = createInputs("100\t1\t3\t881250949\n" +
                                      "100\t2\t5\t881250949\n" +
                                      "100\t3\t5\t881250949\n" +
                                      "101\t1\t2\t891717742\n" +
                                      "101\t2\t5\t891717742\n" +
                                      "101\t3\t2\t891717742\n" +
                                      "102\t1\t4\t891717742\n" +
                                      "102\t3\t5\t891717742\n" +
                                      "102\t2\t1\t878887116");
        Path output = tmpDir.getPath("output");
        Path step1 = tmpDir.getPath("step1");
        Path step2 = tmpDir.getPath("step2");
        ItemSimilarityJobDriver job = new ItemSimilarityJobDriver();
        job.setConf(tmpDir.getDefaultConfiguration());
        int run = job.run(new String[]{inputPath.toString(), step1.toString(), step2.toString(), output.toString()});
        Assert.assertEquals(0, run);
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
