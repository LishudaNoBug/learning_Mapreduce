package myflowBeanSort.PartitionSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myFlowCountReducer extends Reducer<myFlowBean, Text, Text, myFlowBean> {


    @Override
    protected void reduce(myFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //	{2481	24681    30000}  13568436656


        for (Text v : values) {
            context.write(v, key);
        }
    }
}
