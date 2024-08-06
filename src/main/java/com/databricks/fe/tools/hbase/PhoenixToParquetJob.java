package com.databricks.fe.tools.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

public class PhoenixToParquetJob {

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
    job.setOutputValueClass(PhoenixRecordWritable.class);
    // Set the output format for writing Parquet
    job.setOutputFormatClass(ParquetOutputFormat.class);
    ParquetOutputFormat.setWriteSupportClass(job, PhoenixToParquetWriteSupport.class);
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
