package org.apachebeam.samples.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class InMemoryPipeline {
    public static void main(String[] args) {
        String path = new File(".").getPath();
        Pipeline p = Pipeline.create();
        PCollection<CustomerEntity> pList = p.apply(Create.of(getCustomers()));

        PCollection<String> pStrList = pList.apply(
                MapElements.into(TypeDescriptors.strings())
                        .via((CustomerEntity cust) -> cust.getName()));
        pStrList.apply(TextIO.write().to(path.concat("\\src\\data\\customer"))
                .withNumShards(1)
                .withSuffix(".csv"));
        p.run();
    }
    static List<CustomerEntity> getCustomers(){
        List<CustomerEntity> list = new ArrayList<CustomerEntity>();
        list.add(new CustomerEntity("1001","Thiago"));
        list.add(new CustomerEntity("1002","Tayna"));
        list.add(new CustomerEntity("1003","Thais"));
        return list;
    }
}
