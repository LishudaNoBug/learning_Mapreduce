package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// KEYIN, VALUEIN   map阶段输出的key和value

/*
Text:从map段传过来的单词类型
IntWritable: 单词的个数类型，默认是数字型
Text: 最终要输出的汇总关键字类型
IntWritable: 最终要输出的汇总关键字个数类型
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    //这里要注意,重写的reduce方法直接就将map传过来的关键字汇总了
    //且自动根据abcd顺序开始汇总
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // atguigu   aitguigu
        //这里atguigu的values已经是2了。
        for (IntWritable value : values) {
            //IntWriteable.get()=>Int类型转化
            sum += value.get();
        }
        //再把Int转化回去IntWriteable，因为输出类型为IntWriteable
        v.set(sum);
        //写出 atguigu 2
        context.write(key, v);
    }
}
		