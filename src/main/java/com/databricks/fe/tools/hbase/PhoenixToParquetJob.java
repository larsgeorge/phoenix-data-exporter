package com.databricks.fe.tools.hbase;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixToParquetJob {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixToParquetJob.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PhoenixToParquetJob <input table name> <output path>");
            System.exit(-1);
        }

        Configuration conf = HBaseConfiguration.create(new Configuration());
        PhoenixConfigurationUtil.setInputClass(conf, PhoenixRecordWritable.class);
        PhoenixConfigurationUtil.setInputCluster(conf, "localhost:2181");
        PhoenixConfigurationUtil.setInputTableName(conf, args[0]);

        Job job = Job.getInstance(conf, "PhoenixToParquetJob");
        job.setJarByClass(PhoenixToParquetJob.class);
        job.setInputFormatClass(PhoenixInputFormat.class);
        job.setMapperClass(PhoenixToParquetMapper.class);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);
        // Set the output format for writing Parquet
        // job.setOutputFormatClass(ParquetOutputFormat.class);
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        ParquetOutputFormat.setWriteSupportClass(job, AvroWriteSupport.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
        Schema schema;
        PhoenixToParquetHelper helper = new PhoenixToParquetHelper(conf);
        try {
            schema = helper.getAvroSchema();
        } catch (SQLException e) {
            throw new IOException("An error occurred.", e);
        }
        AvroParquetOutputFormat.setSchema(job, schema);
        LOG.info("Determined schema: " + schema);        
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
