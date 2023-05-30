package org.apachebeam.samples.pipeline1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;

public class LocalFilePipeline {
    public static void main(String[] args){
        String path = new File(".").getPath();

        IMyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IMyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
//        PCollection<String> output = pipeline.apply(TextIO.read().from(path.concat("\\src\\data\\pipeline1.csv")));
        PCollection<String> output = pipeline.apply(TextIO.read().from(path.concat(options.getInputFile())));
//        output.apply(TextIO.write().to(path.concat("\\src\\data\\pipeline1_output")).withNumShards(1).withSuffix(".csv"));
        output.apply(TextIO.write().to(path.concat(options.getOutputFile())).withNumShards(1).withSuffix(options.getExtension()));
        pipeline.run();
        System.out.println("Done");
    }
}
