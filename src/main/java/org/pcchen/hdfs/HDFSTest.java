package org.pcchen.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

/**
 * hdfs测试程序
 *
 * 错误处理：
 * 1、对于环境变量中的HADOOP_HOME中hadoop.dll文件以及系统目录system32下的hadoop.dll文件替换：
 *      java.lang.UnsatisfiedLinkError:org.apache.hadoop.util.NativeCrc32.nativeComputeChunkedSumsByteArray；
 * @author ceek
 * @create 2019-11-14 15:06
 **/
public class HDFSTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.10.32.61:8020");
        try {
            System.setProperty("HADOOP_USER_NAME", "chenpeichao");
            FileSystem fs = FileSystem.get(conf);
            //本地文件上传到hdfs和下载到window文件目录
//            fs.copyFromLocalFile(new Path("e:/operationsanalysis.jar"), new Path("/user/chenpeichao/"));
//            fs.copyToLocalFile(false, new Path("/user/chenpeichao/hellospark"), new Path("e:/hellospark"), true);

            //创建文件夹时，对于文件进行权限赋予
            FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.mkdirs(new Path("/user/chenpeichao"), fsPermission);

            //递归删除目录
//            fs.delete(new Path("/user/chenpeichao"), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
