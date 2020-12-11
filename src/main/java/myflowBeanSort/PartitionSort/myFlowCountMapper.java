package myflowBeanSort.PartitionSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//13470253144	180	180	360
public class myFlowCountMapper extends Mapper<LongWritable, Text, myFlowBean, Text> {
    Text v = new Text();
    myFlowBean k = new myFlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] ss = line.split("\t");

        //封装key
        String phoneNumber = ss[0];
        v.set(phoneNumber);

        //封装value
        long upFlow = Long.parseLong(ss[1]);
        long downFlow = Long.parseLong(ss[2]);
        long sumFlow = Long.parseLong(ss[3]);
        k.set(upFlow, downFlow, sumFlow);

        context.write(k, v);
    }
}
