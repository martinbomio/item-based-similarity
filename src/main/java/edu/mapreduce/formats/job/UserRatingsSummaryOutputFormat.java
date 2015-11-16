package edu.mapreduce.formats.job;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Output format for writing user move ratings summary objects. Internally, uses
 * jackson object mapper to serialize the object as json.
 */
public class UserRatingsSummaryOutputFormat extends TextOutputFormat<LongWritable, UserRatingsSummaryWritable> {
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RecordWriter<LongWritable, UserRatingsSummaryWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        FSDataOutputStream outputStream = FileSystem.get(taskAttemptContext.getConfiguration())
                                                    .create(this.getDefaultWorkFile(taskAttemptContext,
                                                                                    ""));
        return new UserRatingsSummaryRecordWriter(outputStream);
    }

    public static class UserRatingsSummaryRecordWriter extends
            RecordWriter<LongWritable, UserRatingsSummaryWritable> {
        private FSDataOutputStream outputStream;

        public UserRatingsSummaryRecordWriter(FSDataOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(LongWritable longWritable,
                          UserRatingsSummaryWritable userRatingsSummaryWritable) throws IOException {
            String value = objectMapper.writeValueAsString(userRatingsSummaryWritable.get()) + "\n";
            outputStream.write(value.getBytes());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException,
                InterruptedException {
            this.outputStream.close();
        }
    }
}
