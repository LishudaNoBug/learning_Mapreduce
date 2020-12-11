package myflowBeanPartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
public class myFlowCountMapper extends Mapper<LongWritable, Text, Text, myFlowBean> {

    Text k = new Text();
    myFlowBean myflowBean = new myFlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] ss = line.split("\t");

        //手机号
        k.set(ss[1]);

        long upFlow = Long.parseLong(ss[ss.length - 3]);
        long downFlow = Long.parseLong(ss[ss.length - 2]);
        myflowBean.set(upFlow, downFlow);

        context.write(k, myflowBean);
    }
}
