package org.apachebeam.samples.pipelines.aws_parquet;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class S3Pipeline {
    public static void main(String[] args) {
        IOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IOptions.class);
        Pipeline p = Pipeline.create(options);

        AWSCredentials awsCredObj = new BasicAWSCredentials(options.getAWSAccessKey(),options.getAWSSecretKey());
        options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredObj));;

        PCollection<String> pInput = p.apply(TextIO.read().from("s3://..csv"));
        pInput.apply(ParDo.of(new DoFn<String, Void>(){
            @ProcessElement
            void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        p.run();
    }
}
