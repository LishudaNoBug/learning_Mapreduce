package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// map阶段
// KEYIN 输入数据的key 
// VALUEIN 输入数据的value
// KEYOUT 输出数据的key的类型   atguigu,1   ss,1
// VALUEOUT 输出的数据的value类型

/*
LongWritable：第x行数
Text：第x行的内容
Text:输出的单词类型
IntWritable：该单词未聚合前的个数，固定是1
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key*************" + key.toString());
        System.out.println("value*************" + value.toString());

        // 1 获取一行内容  ：  atguigu atguigu
        String line = value.toString();
        // 2 将该行切开   ：  [atguigu , atguigu]
        String[] words = line.split(" ");
        // 3 每个单词写到上下文中
        for (String word : words) {
            // string=>text 类型转化
            k.set(word);
            //写到上下文中。上下文就是一个空间留给下一个用的，只是不知道下一个具体会是谁而已。这么解释？
            context.write(k, v);
        }
    }
}
