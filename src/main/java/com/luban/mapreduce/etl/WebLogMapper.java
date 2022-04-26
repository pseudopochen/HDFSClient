package com.luban.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (parseInput(value.toString())) {
            context.write(value, NullWritable.get());
        }

    }

    private boolean parseInput(String line) {
        return line.split(" ").length > 11;
    }
}
