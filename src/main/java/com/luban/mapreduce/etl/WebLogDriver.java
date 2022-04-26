package com.luban.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WebLogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        args = new String[]{"/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/inputlog", "/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/outputweblog"};

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WebLogDriver.class);
        job.setMapperClass(WebLogMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
