package edu.mapreduce.similarity;

import edu.mapreduce.formats.job.UserMoviesRatingJob;
import edu.mapreduce.similarity.job.CreateSimilarityMatrixJob;
import edu.mapreduce.similarity.job.ItemSimilarityJob;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class ItemSimilarityJobDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        String[] newArgs = Arrays.copyOf(args, args.length + 2);
        String out = args[args.length - 1];
        // add temporary directories
        newArgs[args.length - 1] = "/tmp/step1";
        newArgs[args.length] = "/tmp/step2";
        newArgs[args.length + 1] = out;
        // clean temporary directories
        FileSystem.get(new Configuration()).delete(new Path("/tmp/step1"), true);
        FileSystem.get(new Configuration()).delete(new Path("/tmp/step2"), true);
        // run the workflow
        int res = ToolRunner.run(new Configuration(), new ItemSimilarityJobDriver(), newArgs);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String step1 = args[1];
        String step2 = args[2];
        String output = args[3];
        Configuration configuration = this.getConf();
        String[] step1Args = new String[]{input, step1};
        int res = ToolRunner.run(configuration, new UserMoviesRatingJob(), step1Args);
        if (res != 0) {
            System.exit(1);
        }

        String[] step2Args = new String[]{step1, step2};
        res = ToolRunner.run(configuration, new ItemSimilarityJob(), step2Args);
        if (res != 0) {
            System.exit(1);
        }

        String[] step3Args = new String[]{step2, output};
        return ToolRunner.run(configuration, new CreateSimilarityMatrixJob(), step3Args);
    }
}
