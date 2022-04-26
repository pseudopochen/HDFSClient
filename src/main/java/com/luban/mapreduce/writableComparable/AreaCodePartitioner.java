package com.luban.mapreduce.writableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AreaCodePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phone = text.toString();
        String areaCode = phone.substring(0, 3);

        int partition = 0;

        if ("136".equals(areaCode)) {
            partition = 0;
        } else if ("137".equals(areaCode)) {
            partition = 1;
        } else if ("138".equals(areaCode)) {
            partition = 2;
        } else if ("139".equals(areaCode)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
