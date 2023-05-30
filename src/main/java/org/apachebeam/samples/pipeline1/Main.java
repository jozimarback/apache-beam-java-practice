package org.apachebeam.samples.pipeline1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;

public class Main {
    public static void main(String[] args){
        String path = new File(".").getPath();
        Pipeline pipeline = Pipeline.create();
        PCollection<String> output = pipeline.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline1.csv")));
        output.apply(TextIO.write().to(path.concat("\\src\\data\\pipeline1_output.csv")));
        pipeline.run();
        System.out.println("Done");
    }
}
