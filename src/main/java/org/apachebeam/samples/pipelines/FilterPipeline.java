package org.apachebeam.samples.pipelines;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;

class MyFilter implements SerializableFunction<String, Boolean>{

    @Override
    public Boolean apply(String input) {
        return input.toLowerCase().contains("los angeles");
    }
}
public class FilterPipeline {
    public static void main(String[] args) {
        String path = new File(".").getPath();
        Pipeline p = Pipeline.create();
        PCollection<String> pCustomerList = p.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline_pardo.csv")));
        PCollection<String> pOutput = pCustomerList.apply(Filter.by(new MyFilter()));
        pOutput.apply(TextIO.write().to(path.concat("\\src\\data\\filter_output"))
                .withNumShards(1)
                        .withHeader("Id,Name,Last Name,City")
                .withSuffix(".csv"));

        p.run();

    }
}
