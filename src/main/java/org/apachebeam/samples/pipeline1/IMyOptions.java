package org.apachebeam.samples.pipeline1;

import org.apache.beam.sdk.options.PipelineOptions;

public interface IMyOptions extends PipelineOptions {
    void setInputFile(String file);
    String getInputFile();
    void setOutputFile(String file);
    String getOutputFile();

    void setExtension(String extension);
    String getExtension();

}
