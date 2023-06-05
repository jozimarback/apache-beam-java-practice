package org.apachebeam.samples.pipelines.aws_parquet;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

class BeamCustUtil{
    public static Schema getSchema(){
         String SCHEMA_STRING =
            "{\"namespace\":\"org.apachebeam.samples.pipelines.aws_parquet\",\n"
                 +"\"type\":\"record\",\n"
                 +"\"name\":\"ParquetExample\",\n"
                 +"\"fields\":[\n"
                     +"{\"name\":\"SessionId\", \"type\": \"string\"},n"
                     +"{\"name\":\"UserId\", \"type\": \"string\"},n"
                     +"{\"name\":\"UserName\", \"type\": \"string\"},n"
                     +"{\"name\":\"VideoId\", \"type\": \"string\"},n"
                     +"{\"name\":\"Duration\", \"type\": \"int\"},n"
                     +"{\"name\":\"StartedTime\", \"type\": \"string\"},n"
                     +"{\"name\":\"Sex\", \"type\": \"string\"}n"
                 +"}\n"
                 +"}"
                 ;


         Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);
         return SCHEMA;
    }
}

class ConvertCsvToGeneric extends SimpleFunction<String, GenericRecord>{
    @Override
    public GenericRecord apply(String input){
        String arr[] = input.split(",");
        Schema schema = BeamCustUtil.getSchema();
        GenericRecord record = new GenericData.Record(schema);
        record.put("SessionId",arr[0]);
        record.put("UserId",arr[1]);
        record.put("UserName",arr[2]);
        record.put("VideoId",arr[3]);
        record.put("Duration",Integer.parseInt(arr[4]));
        record.put("StartedTime",arr[5]);
        record.put("Sex",arr[6]);
        return record;
    }
}

class PrintElem extends SimpleFunction<GenericRecord, Void>{
    @Override
    public Void apply(GenericRecord input) {
        System.out.println("SessionId: "+input.get("SessionId"));
        System.out.println("UserId: "+input.get("UserId"));
        System.out.println("UserName: "+input.get("UserName"));
        System.out.println("VideoId: "+input.get("VideoId"));
        System.out.println("Duration: "+input.get("Duration"));
        System.out.println("StartedTime: "+input.get("StartedTime"));
        System.out.println("Sex: "+input.get("Sex"));
        return null;
    }
}

public class ParquetPipeline {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        Schema schema = BeamCustUtil.getSchema();

        createParquet(p, schema);
        readParquet(p, schema);

        p.run();
    }

    private static void readParquet(Pipeline p, Schema schema) {
        PCollection<GenericRecord> pOutput = p.apply(ParquetIO.read(schema).from("c:\\...parquet"));
        pOutput.apply(MapElements.via(new PrintElem()));
    }

    private static void createParquet(Pipeline p, Schema schema) {

        PCollection<GenericRecord> pOutput = p
            .apply(TextIO.read().from("C:\\..."))
            .apply(MapElements.via(new ConvertCsvToGeneric()))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

        pOutput.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to("C:\\..")
                .withNumShards(1).withSuffix(".parquet"));
    }
}
