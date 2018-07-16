package com.pponna.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * The KeyValueListWritable.java.
 *
 * @author Pratheesh
 */
public class KeyValueListWritable implements Writable,Serializable {

    private static final long serialVersionUID = 6362979700006820398L;

    private List<KeyValue> keyValueList;

    private Text tableName;
    
    private Text rowKey;

    /**
     * Method to get rowkey
     * @return
     */
    public Text getRowKey() {
        return rowKey;
    }

    /**
     * Method to set rowkey
     * @param rowKey
     */
    public void setRowKey(Text rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * Default Constructor.
     */
    public KeyValueListWritable() {
        keyValueList = new ArrayList<KeyValue>();
        tableName = new Text();
        rowKey = new Text();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(keyValueList.size());
        for (KeyValue keyValue : keyValueList) {
            KeyValue.write(keyValue, out);
        }
        tableName.write(out);
        rowKey.write(out);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        keyValueList = new ArrayList<KeyValue>(size);
        for (int i = 0; i < size; i++) {
            KeyValue keyValue = KeyValue.create(in);

            keyValueList.add(keyValue);
        }
        tableName.readFields(in);
        rowKey.readFields(in);
    }

    /**
     * Method to get keyvalue list
     * @return
     */
    public List<KeyValue> getKeyValueList() {
        return keyValueList;
    }

    /**
     * Method to set keyvalue list
     * @param keyValueList
     */
    public void setKeyValueList(List<KeyValue> keyValueList) {
        this.keyValueList = keyValueList;
    }

    /**
     * Method to get table name
     * @return
     */
    public Text getTableName() {
        return tableName;
    }

    /**
     * Method to set table name
     * @param tableName
     */
    public void setTableName(Text tableName) {
        this.tableName = tableName;
    }

}
