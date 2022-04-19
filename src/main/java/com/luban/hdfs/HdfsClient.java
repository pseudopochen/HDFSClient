package com.luban.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


/**
 * 1. get the client
 * 2. carry out the operation
 * 3. close the resource
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://localhost:9000");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        String user = "pochen";
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    /**
     * configuration parameter priority:
     * hdfs-default.xml => hdfs-site.xml => resources/hdfs-siete.xml => configuration.set
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        // arg1: delete source file
        // arg2: overwrite target file
        // arg3: source path
        // arg4: target path
        fs.copyFromLocalFile(false, false, new Path("/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    @Test
    public void testGet() throws IOException {
        // arg1: delete source file
        // arg2: source file hdfs path
        // arg3: target local path
        // arg4: ignore CRC check
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan"), new Path("/mnt/gv0/brick/modules/hadoop/hadoop-3.3.2/"), true);
    }

    @Test
    public void testRm() throws IOException {
        // arg1: hdfs path to delete
        // arg2: recursive for directory
        fs.delete(new Path("/xiyou"), true);
    }

    @Test
    public void testmv() throws IOException {
        fs.rename(new Path("/input/word.txt"), new Path("/words.txt"));
    }

    @Test
    public void fileDetails() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus f = listFiles.next();
            System.out.println("=========" + f.getPath() + "==========");
            System.out.println(f.getPermission());
            System.out.println(f.getOwner());
            System.out.println(f.getGroup());
            System.out.println(f.getLen());
            System.out.println(f.getModificationTime());
            System.out.println(f.getReplication());
            System.out.println(f.getBlockSize());
            System.out.println(f.getPath().getName());

            // get block info
            BlockLocation[] blockLocations = f.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("File: " + fileStatus.getPath().getName());
            } else {
                System.out.println("Directory: " + fileStatus.getPath().getName());
            }
        }
    }
}


