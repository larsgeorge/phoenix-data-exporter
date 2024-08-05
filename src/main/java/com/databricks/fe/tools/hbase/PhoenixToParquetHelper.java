package com.databricks.fe.tools.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

    public Schema convertPColumnToParquetColumn(final PColumn col) {
        Schema s = null;
        @SuppressWarnings("rawtypes")
        PDataType pDataType = col.getDataType(); 
        if (pDataType instanceof PBinary) {
            s = Schema.create(Schema.Type.BYTES);
        } else if (pDataType instanceof PBinaryArray) {
            s = Schema.create(Schema.Type.BYTES);
        } else if (pDataType instanceof PBoolean) {
            s = Schema.create(Schema.Type.BOOLEAN);
        } else if (pDataType instanceof PBooleanArray) {
            s = Schema.create(Schema.Type.BOOLEAN);
        } else if (pDataType instanceof PChar) {
            s = Schema.create(Schema.Type.STRING);
        } else if (pDataType instanceof PCharArray) {
            s = Schema.create(Schema.Type.STRING);
        } else if (pDataType instanceof PDate) {
            s = Schema.create(Schema.Type.LONG);
            // types.add(PUnsignedDate.INSTANCE);
            // types.add(PUnsignedDateArray.INSTANCE);
        } else if (pDataType instanceof PDateArray) {
            s = Schema.create(Schema.Type.LONG);
        } else if (pDataType instanceof PDecimal) {
            s = LogicalTypes.decimal(col.getMaxLength(), col.getScale()).addToSchema(Schema.create(Schema.Type.BYTES)); 
        } else if (pDataType instanceof PDecimalArray) {
            s = Schema.create(Schema.Type.BYTES);
        } else if (pDataType instanceof PDouble || pDataType instanceof PUnsignedDouble) {
            s = Schema.create(Schema.Type.DOUBLE);
        } else if (pDataType instanceof PDoubleArray || pDataType instanceof PUnsignedDoubleArray) {
            s = Schema.create(Schema.Type.DOUBLE);
        } else if (pDataType instanceof PFloat || pDataType instanceof PUnsignedFloat) {
            s = Schema.create(Schema.Type.FLOAT);
        } else if (pDataType instanceof PFloatArray || pDataType instanceof PUnsignedFloatArray) {
            s = Schema.create(Schema.Type.FLOAT);
        } else if (pDataType instanceof PInteger || pDataType instanceof PUnsignedInt) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PIntegerArray || pDataType instanceof PUnsignedIntArray) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PLong || pDataType instanceof PUnsignedLong) {
            s = Schema.create(Schema.Type.LONG);
        } else if (pDataType instanceof PLongArray || pDataType instanceof PUnsignedLongArray) {
            s = Schema.create(Schema.Type.LONG);
        } else if (pDataType instanceof PSmallint || pDataType instanceof PUnsignedSmallint) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PSmallintArray || pDataType instanceof PUnsignedSmallintArray) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PTime || pDataType instanceof PUnsignedTime) {
            s = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (pDataType instanceof PTimeArray || pDataType instanceof PUnsignedTimeArray) {
            s = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (pDataType instanceof PTimestamp || pDataType instanceof PUnsignedTimestamp) {
            s = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (pDataType instanceof PTimestampArray || pDataType instanceof PUnsignedTimestampArray) {
            s = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (pDataType instanceof PTinyint || pDataType instanceof PUnsignedTinyint) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PTinyintArray || pDataType instanceof PUnsignedTinyintArray) {
            s = Schema.create(Schema.Type.INT);
        } else if (pDataType instanceof PVarbinary) {
            s = Schema.create(Schema.Type.BYTES);
        } else if (pDataType instanceof PVarbinaryArray) {
            s = Schema.create(Schema.Type.BYTES);
        } else if (pDataType instanceof PVarchar) {
            s = Schema.create(Schema.Type.STRING);
        } else if (pDataType instanceof PVarcharArray) {
            s = Schema.create(Schema.Type.STRING);
        }  
        if (//pDataType.isNullable() && 
            s != null)
            s = Schema.createUnion(List.of(s, Schema.create(Schema.Type.NULL)));
        return s;
    }

    public Schema getAvroSchema(List<PColumn> cols) {
        List<Schema.Field> flds = new ArrayList<Schema.Field>();
        LOG.info("Path: " + Schema.Field.class.getProtectionDomain().getCodeSource().getLocation()); 
        for (PColumn col : cols) {
            Schema schema = convertPColumnToParquetColumn(col);
            LOG.info("Data type: " + col.getDataType() + " -> Converted schema: " + schema);
            LOG.info("Column Nullable: " + col.getDataType().isNullable());
            if (schema != null) {
                Schema.Field fld = new Schema.Field(col.getName().toString(), schema, null, (Object) null);
                flds.add(fld);
            }
        }
        LOG.debug("Number of converted columns: " + flds.size());
        Schema row = Schema.createRecord("row", null, null, false);
        row.setFields(flds);
        return row;
    }

    public Schema getAvroSchema() throws SQLException {
        LinkedHashMap<String, PColumn> cols = getPColumns();
        List<PColumn> pCols = new ArrayList<>();
        for (String colName : cols.keySet()) { 
            PColumn col = cols.get(colName);
            pCols.add(col);
        }
        Schema schema = getAvroSchema(pCols);
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
            Schema schema = helper.getAvroSchema(pCols);
            System.out.println(schema);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


