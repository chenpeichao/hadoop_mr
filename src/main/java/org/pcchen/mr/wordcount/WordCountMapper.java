package org.pcchen.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计单词个数mapper
 *
 * @author ceek
 * @create 2019-11-15 10:06
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] split = line.toString().split("\t");
        for (String word : split) {
            text.set(word);
            context.write(text, new IntWritable(1));
        }
    }
}