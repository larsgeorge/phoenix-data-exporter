package com.databricks.fe.tools.hbase;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;

import java.io.IOException;

public class PhoenixToParquetMapper extends Mapper<NullWritable, PhoenixRecordWritable, Void, PhoenixRecordWritable> {

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void map(NullWritable key, PhoenixRecordWritable value, Context context)
      throws IOException, InterruptedException {
    context.write(null, value);
  }
}
