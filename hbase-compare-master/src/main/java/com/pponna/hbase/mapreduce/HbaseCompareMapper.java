package com.pponna.hbase.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HbaseCompareMapper.java.
 *
 * @author Pratheesh
 */
public class HbaseCompareMapper extends TableMapper<ImmutableBytesWritable, KeyValueListWritable> {

    private static final Logger MAPLOGGER = LoggerFactory.getLogger(HbaseCompareMapper.class);

    private static Set<String> valueOnlyFieldSet = new HashSet<String>();

    private static Set<String> skipFieldsSet = new HashSet<String>();

    private static Set<String> skipColFamilySet = new HashSet<String>();

    private static boolean skipTS = false;

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {

        TableSplit currentSplit = (TableSplit) context.getInputSplit();
        byte[] tableName = currentSplit.getTableName();

        try {
            KeyValueListWritable keyValueListWritable = resultToPut(key, value);
            keyValueListWritable.setTableName(new Text(tableName));
            context.write(key, keyValueListWritable);
        } catch (Exception e) {
            MAPLOGGER.info(org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
    }

    /**
     * Method to add hbase results to put object
     * @param key
     * @param result
     * @return
     * @throws IOException
     */
    private static KeyValueListWritable resultToPut(ImmutableBytesWritable key, Result result) throws IOException {

        KeyValueListWritable keyValueListWritable = new KeyValueListWritable();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
        Set<byte[]> colFamilySet = navigableMap.keySet();
        for (byte[] colFamily : colFamilySet) {
            if (skipColFamilySet.contains(Bytes.toString(colFamily))) {
                continue;
            }
            NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap = navigableMap.get(colFamily);
            Set<byte[]> qualifierSet = qualifierMap.keySet();
            for (byte[] qualifier : qualifierSet) {
                if (skipFieldsSet.contains(Bytes.toString(qualifier))) {
                    continue;
                }
                NavigableMap<Long, byte[]> timeStampMap = qualifierMap.get(qualifier);
                Set<Long> timeStampSet = timeStampMap.keySet();
                for (Long timestamp : timeStampSet) {
                    long valueTimestamp = timestamp;
                    if (valueOnlyFieldSet.contains(Bytes.toString(qualifier)) || skipTS) {
                        valueTimestamp = 0;
                    }
                    byte[] value = timeStampMap.get(timestamp);
                    byte[] rowkey = key.get();
                    keyValueListWritable.setRowKey(new Text(rowkey));
                    KeyValue kv = new KeyValue(rowkey, colFamily, qualifier, valueTimestamp, value);
                    keyValueListWritable.getKeyValueList().add(kv);
                }
            }
        }
        return keyValueListWritable;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String valueOnlyFields = context.getConfiguration().get("ValueOnlyFields");
        if (valueOnlyFields != null) {
            String[] valueOnlyFieldsArr = valueOnlyFields.split(",");
            for (String string : valueOnlyFieldsArr) {
                if (!StringUtils.isBlank(string)) {
                    valueOnlyFieldSet.add(string);
                }
            }
        }
        String skipFields = context.getConfiguration().get("SkipFields");
        if (skipFields != null) {
            String[] skipFieldsArr = skipFields.split(",");
            for (String string : skipFieldsArr) {
                if (!StringUtils.isBlank(string)) {
                    skipFieldsSet.add(string);
                }
            }
        }
        String skipColFamily = context.getConfiguration().get("SkipColFamily");
        if (skipColFamily != null) {
            String[] skipColFamilyArr = skipColFamily.split(",");
            for (String string : skipColFamilyArr) {
                if (!StringUtils.isBlank(string)) {
                    skipColFamilySet.add(string);
                }
            }
        }
        skipTS = Boolean.parseBoolean(context.getConfiguration().get("skipTS"));
    }

}
