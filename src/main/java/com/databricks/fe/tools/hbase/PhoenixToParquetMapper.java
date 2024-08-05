package com.databricks.fe.tools.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.io.IOException;

public class PhoenixToParquetMapper extends Mapper<NullWritable, PhoenixRecordWritable, Void, GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixToParquetMapper.class);
    static final String AVRO_SCHEMA = "parquet.avro.schema";

    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = new Schema.Parser().parse(conf.get(AVRO_SCHEMA));
        LOG.info("Mapper received Avro schema: " + schema);
    }

    @Override
    protected void map(NullWritable key, PhoenixRecordWritable value, Context context) 
    throws IOException, InterruptedException {
        GenericRecord record = new GenericData.Record(schema);
        for (Field field : schema.getFields()) {
            record.put(field.name(), value.getResultMap().get(field.name()));
        }
        LOG.info("Record: " + record);
        context.write(null, record);
    }
}
