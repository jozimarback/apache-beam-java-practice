package org.apachebeam.samples.pipelines.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;


import java.util.Map;

public class IotDeserializer implements Deserializer<IotEvent> {
    @Override
    public void close() {
        Deserializer.super.close();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public IotEvent deserialize(String s, byte[] bytes) {
        ObjectMapper om = new ObjectMapper();
        IotEvent iotEvent = null;
        try {
            iotEvent = om.readValue(bytes, IotEvent.class);
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
        return iotEvent;
    }
}
