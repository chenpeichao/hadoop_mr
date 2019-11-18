package org.pcchen.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计单词个数Redicer
 *
 * @author ceek
 * @create 2019-11-18 11:03
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable sumIntWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
        }

        sumIntWritable.set(sum);
        context.write(key, sumIntWritable);
    }
}
