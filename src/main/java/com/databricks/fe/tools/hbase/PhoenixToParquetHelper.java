package com.databricks.fe.tools.hbase;

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
// import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
// import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
// import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
// import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PBooleanArray;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PCharArray;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDateArray;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDecimalArray;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PDoubleArray;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PFloatArray;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PLongArray;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PSmallintArray;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimeArray;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTimestampArray;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PTinyintArray;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedDoubleArray;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedFloatArray;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedLongArray;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedSmallintArray;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimeArray;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestampArray;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PUnsignedTinyintArray;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryArray;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixToParquetHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PhoenixToParquetHelper.class);

  Configuration conf;

  public PhoenixToParquetHelper(Configuration conf) {
    this.conf = conf;
  }

  public LinkedHashMap<String, PColumn> getPColumns() throws SQLException {
    String connectionString = "jdbc:phoenix:" + PhoenixConfigurationUtil.getInputCluster(conf);
    String tableName = PhoenixConfigurationUtil.getInputTableName(conf);
    String schemaName = "";
    Connection con = DriverManager.getConnection(connectionString);
    PhoenixConnection phoenixConnection = con.unwrap(PhoenixConnection.class);
    MetaDataClient metaDataClient = new MetaDataClient(phoenixConnection);
    PTable table = metaDataClient.updateCache(schemaName, tableName).getTable();
    LinkedHashMap<String, PColumn> pColumns = new LinkedHashMap<String, PColumn>();
    for (PColumn pColumn : table.getColumns()) {
      LOG.debug("PColumn: " + pColumn);
      String colName = pColumn.getName().getString();
      if (!colName.equals(SaltingUtil.SALTING_COLUMN_NAME)) {
        pColumns.put(colName, pColumn);
      } else {
        LOG.info("Skipping column: " + colName);
      }
    }
    return pColumns;
  }

  public Type convertPColumnToParquetColumn(final PColumn col) {
    Types.PrimitiveBuilder<PrimitiveType> builder = null;
    @SuppressWarnings("rawtypes")
    PDataType pDataType = col.getDataType();
    Repetition repetition = col.isNullable() ? OPTIONAL : REQUIRED;
    if (pDataType instanceof PBinary) {
      builder = Types.primitive(BINARY, repetition);
    } else if (pDataType instanceof PBinaryArray) {
      // TODO
    } else if (pDataType instanceof PBoolean) {
      builder = Types.primitive(BOOLEAN, repetition);
    } else if (pDataType instanceof PBooleanArray) {
      // TODO
    } else if (pDataType instanceof PChar) {
      builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(stringType());
    } else if (pDataType instanceof PCharArray) {
      // TODO
    } else if (pDataType instanceof PDate) {
      builder = Types.primitive(INT64, repetition).as(dateType());
      // types.add(PUnsignedDate.INSTANCE);
      // types.add(PUnsignedDateArray.INSTANCE);
    } else if (pDataType instanceof PDateArray) {
      // TODO
    } else if (pDataType instanceof PDecimal) {
      builder = Types.primitive(BINARY, repetition).as(decimalType(col.getMaxLength(), col.getScale()));
    } else if (pDataType instanceof PDecimalArray) {
      // TODO
    } else if (pDataType instanceof PDouble || pDataType instanceof PUnsignedDouble) {
      builder = Types.primitive(DOUBLE, repetition);
    } else if (pDataType instanceof PDoubleArray || pDataType instanceof PUnsignedDoubleArray) {
      // TODO
    } else if (pDataType instanceof PFloat || pDataType instanceof PUnsignedFloat) {
      builder = Types.primitive(FLOAT, repetition);
    } else if (pDataType instanceof PFloatArray || pDataType instanceof PUnsignedFloatArray) {
      // TODO
    } else if (pDataType instanceof PInteger || pDataType instanceof PUnsignedInt) {
      builder = Types.primitive(INT32, repetition);
    } else if (pDataType instanceof PIntegerArray || pDataType instanceof PUnsignedIntArray) {
      // TODO
    } else if (pDataType instanceof PLong || pDataType instanceof PUnsignedLong) {
      builder = Types.primitive(INT64, repetition);
    } else if (pDataType instanceof PLongArray || pDataType instanceof PUnsignedLongArray) {
      // TODO
    } else if (pDataType instanceof PSmallint || pDataType instanceof PUnsignedSmallint) {
      builder = Types.primitive(INT32, repetition);
    } else if (pDataType instanceof PSmallintArray || pDataType instanceof PUnsignedSmallintArray) {
      // TODO
    } else if (pDataType instanceof PTime || pDataType instanceof PUnsignedTime) {
      builder = Types.primitive(INT64, repetition).as(timeType(true, MILLIS));
    } else if (pDataType instanceof PTimeArray || pDataType instanceof PUnsignedTimeArray) {
      // TODO
    } else if (pDataType instanceof PTimestamp || pDataType instanceof PUnsignedTimestamp) {
      builder = Types.primitive(INT64, repetition).as(timestampType(true, MILLIS));
    } else if (pDataType instanceof PTimestampArray || pDataType instanceof PUnsignedTimestampArray) {
      // TODO
    } else if (pDataType instanceof PTinyint || pDataType instanceof PUnsignedTinyint) {
      builder = Types.primitive(INT32, repetition);
    } else if (pDataType instanceof PTinyintArray || pDataType instanceof PUnsignedTinyintArray) {
      // TODO
    } else if (pDataType instanceof PVarbinary) {
      builder = Types.primitive(BINARY, repetition).as(stringType());
    } else if (pDataType instanceof PVarbinaryArray) {
      // TODO
    } else if (pDataType instanceof PVarchar) {
      builder = Types.primitive(BINARY, repetition).as(stringType());
    } else if (pDataType instanceof PVarcharArray) {
      // TODO
    } else {
      throw new IllegalStateException("Invalid Phoenix data type: " + pDataType);
    }
    return builder != null ? builder.named(col.getName().getString()) : null;
  }

  public MessageType getParquetSchema(List<PColumn> cols) {
    List<Type> flds = new ArrayList<Type>();
    for (PColumn col : cols) {
      Type type = convertPColumnToParquetColumn(col);
      LOG.info("Data type: " + col.getDataType() + " -> Converted type: " + type);
      LOG.info("Column Nullable: " + col.isNullable());
      if (type != null) {
        flds.add(type);
      }
    }
    LOG.debug("Number of converted columns: " + flds.size());
    return new MessageType("row", flds);
  }

  public MessageType getParquetSchema() throws SQLException {
    LinkedHashMap<String, PColumn> cols = getPColumns();
    List<PColumn> pCols = new ArrayList<>();
    for (String colName : cols.keySet()) {
      PColumn col = cols.get(colName);
      pCols.add(col);
    }
    MessageType schema = getParquetSchema(pCols);
    return schema;
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    PhoenixConfigurationUtil.setInputCluster(conf, "localhost:2181");
    PhoenixConfigurationUtil.setInputTableName(conf, "JAVATEST");
    PhoenixToParquetHelper helper = new PhoenixToParquetHelper(conf);
    try {
      LinkedHashMap<String, PColumn> cols = helper.getPColumns();
      List<PColumn> pCols = new ArrayList<>();
      for (String colName : cols.keySet()) {
        PColumn col = cols.get(colName);
        System.out.println("Column: " + colName + " -> " + col.getDataType());
        pCols.add(col);
      }
      MessageType schema = helper.getParquetSchema(pCols);
      System.out.println(schema);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
