package org.apachebeam.samples.pipelines.aws_parquet;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;

public interface IOptions extends PipelineOptions, S3Options {
    void setAWSAccessKey(String val);
    String getAWSAccessKey();

    void setAWSSecretKey(String val);
    String getAWSSecretKey();

    void setAWSRegion(String val);
    String getAWSRegion();
}
