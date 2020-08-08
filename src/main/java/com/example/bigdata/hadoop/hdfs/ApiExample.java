package com.example.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * hdfs api 例子
 */
public class ApiExample {

    private FileSystem fs = null;

    @Before
    public void before() throws Exception {
        Configuration conf = new Configuration();
        // 设置副本数为1
        conf.set("dfs.replication","1");
        fs = FileSystem.get(new URI("hdfs://hadoop001:8020"),conf,"hadoop");
    }

    @After
    public void after() throws Exception {
        fs.close();
    }

    /**
     * 创建文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/aa/bb");
        boolean res = fs.mkdirs(path);
        System.out.println(res);
    }

    /**
     * 查看文件内容
     * @throws Exception
     */
    @Test
    public void text() throws Exception {
        FSDataInputStream in = fs.open(new Path("/README.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }


    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fs.create(new Path("/aa/bb/b.txt"));
        out.writeUTF("你好 Hadoop!!");
        out.flush();
        out.close();
    }

    /**
     * 重命名文件
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        boolean res = fs.rename(new Path("/aa/bb/b.txt"),new Path("/aa/bb/c.txt"));
        System.out.println(res);
    }

    /**
     * 拷贝文件从本地
     * @throws Exception
     */
    @Test
    public void copyFileFromLocal() throws Exception {
        fs.copyFromLocalFile(new Path("/Users/perl/Desktop/aa.txt"),new Path("/aa/bb/aa.txt"));
    }

    /**
     * 下载hdfs文件到本地
     * @throws Exception
     */
    @Test
    public void copyFileToLocal() throws Exception {
        fs.copyToLocalFile(new Path("/aa/bb/c.txt"),new Path("/Users/perl/Desktop/cc.txt"));
    }

    /**
     * 查看文件/文件夹信息
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/aa/bb/a.txt"));
        for (FileStatus status : fileStatuses) {
            System.out.println(status);
        }
    }

    /**
     * 递归查看文件夹
     * @throws Exception
     */
    @Test
    public void listRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/aa"), true);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    /**
     * 查看文件块信息
     * @throws Exception
     */
    @Test
    public void blockInfo() throws Exception {
        FileStatus fileStatus = fs.getFileStatus(new Path("/aa/bb/a.txt"));
        BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation location : locations) {
            System.out.println(location);
        }
    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void deleteFile() throws Exception {
        boolean res = fs.delete(new Path("/aa/bb/c.txt"),false);
        System.out.println(res);
    }
}
