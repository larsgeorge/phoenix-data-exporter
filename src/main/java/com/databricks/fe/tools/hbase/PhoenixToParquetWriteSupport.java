package com.databricks.fe.tools.hbase;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.phoenix.mapreduce.PhoenixRecordWritable;
import org.apache.phoenix.schema.PColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixToParquetWriteSupport extends WriteSupport<PhoenixRecordWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(PhoenixToParquetWriteSupport.class);

  private Map<String, PColumn> pColumns;
  private MessageType schema;
  private RecordConsumer recordConsumer;

  @Override
  public WriteContext init(Configuration configuration) {
    PhoenixToParquetHelper helper = new PhoenixToParquetHelper(configuration);
    try {
      pColumns = helper.getPColumns();
      schema = helper.getParquetSchema(new ArrayList<>(pColumns.values()));
      LOG.info("Schema: " + schema);
    } catch (SQLException e) {
      throw new RuntimeException("An error occurred.", e);
    }
    Map<String, String> extraMetaData = new HashMap<String, String>();
    return new WriteContext(schema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(PhoenixRecordWritable record) {
    recordConsumer.startMessage();
    LOG.info("Writing record...");
    writeRecordFields(record);
    recordConsumer.endMessage();
  }

  private void writeRecordFields(PhoenixRecordWritable record) {
    int index = 0;
    LOG.info("Num flds: " + schema.getFields().size());
    for (Type fld : schema.getFields()) {
      Object value = record.getResultMap().get(fld.getName());
      LOG.info("Index: " + index);
      LOG.info("Field: " + fld);
      LOG.info("Object class: " + (value != null ? value.getClass() : null));
      LOG.info("Object value: " + value);
      if (value != null) {
        recordConsumer.startField(fld.getName(), index);
        writeValue(fld, value);
        recordConsumer.endField(fld.getName(), index);
      }
      index++;
    }
  }

  private void writeValue(Type field, Object value) {
    LogicalTypeAnnotation ann = field.asPrimitiveType().getLogicalTypeAnnotation();
    switch (field.asPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) value);
        break;
      case INT32:
        recordConsumer.addInteger(((Number) value).intValue());
        break;
      case INT64:
      case INT96:
        if (ann instanceof TimeLogicalTypeAnnotation) {
          recordConsumer.addLong(((Time) value).getTime());
        } else if (ann instanceof TimestampLogicalTypeAnnotation) {
          recordConsumer.addLong(((Timestamp) value).getTime()); // TODO: Handle nanos
        } else if (ann instanceof DateLogicalTypeAnnotation) {
          recordConsumer.addLong(((Date) value).getTime());
        } else {
          recordConsumer.addLong(((Number) value).longValue());
        }
        break;
      case FLOAT:
        recordConsumer.addFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        recordConsumer.addDouble(((Number) value).doubleValue());
        break;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        if (value instanceof byte[]) {
          recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
        } else if (field.asPrimitiveType().getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation) {
          recordConsumer.addBinary(Binary.fromString((String) value));
        } else if (field.asPrimitiveType().getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
          recordConsumer.addDouble(((BigDecimal) value).floatValue());
        } else {
          recordConsumer.addBinary(Binary.fromReusedByteArray(value.toString().getBytes()));
        }
        break;
      default:
        throw new IllegalStateException("Invalid Parquet field type.");
    }
  }

}
