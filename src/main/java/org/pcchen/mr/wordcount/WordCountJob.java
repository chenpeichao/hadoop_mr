package org.pcchen.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 统计单词个数job
 *
 * @author ceek
 * @create 2019-11-18 11:06
 **/
public class WordCountJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.10.32.61:8020");
        System.setProperty("HADOOP_USER_NAME", "chenpeichao");

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountJob.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, "/user/chenpeichao/wordcount/input");
        FileOutputFormat.setOutputPath(job, new Path("/user/chenpeichao/wordcount/sum"));

        job.setJobName("wordcount_job");

        boolean res = job.waitForCompletion(true);


        System.exit(res ? 0 : 1);
    }

//    public static void main(String[] args) {
//        Configuration conf = new Configuration();
//
//        conf.set("fs.defaultFS", "hdfs://10.10.32.61:8020");
//		/*conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "mini1");*/
//
//        Job job = null;
//        try {
//            job = Job.getInstance(conf);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        //告诉框架，我们的程序所在jar包的路径
//        // job.setJar("c:/wordcount.jar");
//        job.setJarByClass(WordCountJob.class);
//
//
//        //告诉框架，我们的程序所用的mapper类和reducer类
//        job.setMapperClass(WordCountMapper.class);
//        job.setReducerClass(WordCountReducer.class);
//
//        //告诉框架，我们的mapperreducer输出的数据类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//
//        // 告诉框架，我们的数据读取、结果输出所用的format组件
//        // TextInputFormat是mapreduce框架中内置的一种读取文本文件的输入组件
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        try {
//            // 告诉框架，我们要处理的文件在哪个路径下
//            FileInputFormat.setInputPaths(job, new Path("/user/chenpeichao/wordcount/input/"));
//            // 告诉框架，我们的处理结果要输出到哪里去
//            FileOutputFormat.setOutputPath(job, new Path("/user/chenpeichao/wordcount/output/"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        boolean res = false;
//        try {
//            res = job.waitForCompletion(true);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//
//        System.exit(res?0:1);
//    }
}
