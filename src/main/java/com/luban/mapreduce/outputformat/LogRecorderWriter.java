package com.luban.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecorderWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream ou1;
    private FSDataOutputStream ou2;

    public LogRecorderWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            ou1 = fs.create(new Path("/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/outputformat/ou1.txt"));
            ou2 = fs.create(new Path("/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/outputformat/ou2.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        String log = text.toString();
        if(log.contains("atguigu")) {
            ou1.writeBytes(log + "\n");
        } else {
            ou2.writeBytes(log + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(ou1);
        IOUtils.closeStream(ou2);
    }
}
