package com.luban.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AreaCodePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
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
