package org.apachebeam.samples.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.io.File;

class MyCityPartition implements Partition.PartitionFn<String>{

    @Override
    public int partitionFor(String elem, int numPartitions) {
        String arr[] = elem.split(",");
        if(arr[3].equals("Los Angeles")){
            return 0;
        }
        else if(arr[3].equals("London")){
            return 1;
        }
        return 2;
    }
}
public class PartitionPipeline {
    public static void main(String[] args) {
        String path = new File(".").getPath();
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline_partition.csv")));

        PCollectionList<String> partition = pCustomerList.apply(Partition.of(3,new MyCityPartition()));

        PCollection<String> p0 = partition.get(0);
        PCollection<String> p1 = partition.get(1);
        PCollection<String> p2 = partition.get(2);

        p0.apply(TextIO.write().to(path.concat("\\src\\data\\partition_0")).withNumShards(1).withSuffix(".csv"));
        p1.apply(TextIO.write().to(path.concat("\\src\\data\\partition_1")).withNumShards(1).withSuffix(".csv"));
        p2.apply(TextIO.write().to(path.concat("\\src\\data\\partition_2")).withNumShards(1).withSuffix(".csv"));
        p.run();
    }
}
