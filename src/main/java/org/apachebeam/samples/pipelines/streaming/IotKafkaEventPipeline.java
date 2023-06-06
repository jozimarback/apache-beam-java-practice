package org.apachebeam.samples.pipelines.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.LongDeserializer;

public class IotKafkaEventPipeline {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        p.apply(KafkaIO.<Long, IotEvent>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("beamtopic")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(IotDeserializer.class)
                .withoutMetadata()
        )
                .apply(Values.<IotEvent>create())
                .apply(ParDo.of(new DoFn<IotEvent, Void>() {
                    public void processElement(ProcessContext c){
                        System.out.println(c.element().getDeviceId());
                    }
                }))
        ;

        p.run();
    }
}
