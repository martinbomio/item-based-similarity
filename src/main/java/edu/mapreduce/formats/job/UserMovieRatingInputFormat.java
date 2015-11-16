package edu.mapreduce.formats.job;

import edu.mapreduce.formats.UserMovieRating;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input format for reading user movie ratings form MovieLens one line at a time.
 */
public class UserMovieRatingInputFormat extends
        FileInputFormat<LongWritable, UserMovieRatingWritable> {
    private static final Logger logger = LoggerFactory.getLogger(UserMovieRatingInputFormat.class);

    @Override
    public RecordReader<LongWritable, UserMovieRatingWritable> createRecordReader(InputSplit inputSplit,
                                                                                  TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new UserMovieRatingRecordReader();
    }

    public static class UserMovieRatingRecordReader extends
            RecordReader<LongWritable, UserMovieRatingWritable> {
        BufferedReader reader;
        UserMovieRating current;
        long fileLength = 0;
        long bytesRead = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) inputSplit;
            fileLength = split.getLength();
            logger.info("Reading from file: " + split.getPath());
            FileSystem fs = FileSystem.get(split.getPath().toUri(),
                                           taskAttemptContext.getConfiguration());
            reader = new BufferedReader(new InputStreamReader(fs.open(split.getPath())));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            String line = this.reader.readLine();
            if (line != null) {
                current = new UserMovieRating(line);
                bytesRead += line.getBytes().length;
            }
            return line != null;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.current.getUserId());
        }

        @Override
        public UserMovieRatingWritable getCurrentValue() throws IOException, InterruptedException {
            return new UserMovieRatingWritable(current);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return bytesRead / fileLength;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
