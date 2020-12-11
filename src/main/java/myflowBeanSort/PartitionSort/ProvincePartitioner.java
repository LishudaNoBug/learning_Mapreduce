package myflowBeanSort.PartitionSort;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ProvincePartitioner extends Partitioner<myFlowBean, Text> {
    @Override
    public int getPartition(myFlowBean key, Text value, int numberPartrtions) {

        //分区规则：

        //手机号前三位
        String prePhoneNumber = value.toString().substring(0, 3);
        numberPartrtions = 4;
        if (prePhoneNumber.equals("136")) {
            numberPartrtions = 0;
        } else if (prePhoneNumber.equals("137")) {
            numberPartrtions = 1;
        } else if (prePhoneNumber.equals("138")) {
            numberPartrtions = 2;
        } else if (prePhoneNumber.equals("139")) {
            numberPartrtions = 3;
        }
        //如果上面四个都不属于就会全部放到4号分区即第5个分区


        return numberPartrtions;
    }
}
