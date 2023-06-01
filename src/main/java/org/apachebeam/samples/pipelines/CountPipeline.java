package org.apachebeam.samples.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;

public class CountPipeline {
    public static void main(String[] args) {
        String path = new File(".").getPath();
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline_partition.csv")));

        PCollection<Long> pLong = pCustomerList.apply(Count.globally());
        pLong.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }

        ));
        p.run();

    }
}
