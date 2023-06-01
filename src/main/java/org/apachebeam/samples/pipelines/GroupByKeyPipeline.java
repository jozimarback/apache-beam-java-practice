package org.apachebeam.samples.pipelines;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.util.Arrays;

import static java.lang.ref.Cleaner.create;

class StringToKV extends DoFn<String, KV<String, Integer>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();
        String arr[] = input.split(",");
        c.output(KV.of(arr[0], Integer.valueOf(arr[3])));
    }
}

class KVToString extends DoFn<KV<String, Iterable<Integer>>,String>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String key = c.element().getKey();
        Iterable<Integer> values = c.element().getValue();

        Integer sum = 0;
        for (Integer val : values) sum += val;

        c.output(key+","+sum.toString());
    }
}
public class GroupByKeyPipeline {
    public static void main(String[] args) {
        String path = new File(".").getPath();
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline_group_by_key.csv")));

        PCollection<KV<String, Integer>> kvOrder = pCustomerList.apply(ParDo.of(new StringToKV()));

        PCollection<KV<String, Iterable<Integer>>> kvGroup = kvOrder.apply(GroupByKey.<String, Integer>create());
        PCollection<String> output = kvGroup.apply(ParDo.of(new KVToString()));

        output.apply(TextIO.write().to(path.concat("\\src\\data\\group_by_output"))
                .withNumShards(1)
                .withHeader("Id,Amount")
                .withSuffix(".csv"));

        p.run();
    }
}
