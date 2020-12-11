package myflowBeanPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myFlowCountReducer extends Reducer<Text, myFlowBean, Text, myFlowBean> {

    myFlowBean sumFlowbean = new myFlowBean();

    @Override
    protected void reduce(Text key, Iterable<myFlowBean> values, Context context) throws IOException, InterruptedException {
        //		13568436656	2481	24681    30000

        long upFlow = 0;
        long downFlow = 0;

        for (myFlowBean myflowBean : values) {
            upFlow += myflowBean.getUpFlow();
            downFlow += myflowBean.getDownFlow();
        }
        sumFlowbean.set(upFlow, downFlow);

        context.write(key, sumFlowbean);
    }
}
