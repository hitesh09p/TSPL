package com.pponna.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The HbaseCompareReducer.java.
 *
 * @author Pratheesh
 */
public class HbaseCompareReducer extends Reducer<ImmutableBytesWritable, KeyValueListWritable, Text, Text> {

    private static byte[] table1Name = null;

    private static byte[] table2Name = null;

    private MultipleOutputs<Text, Text> multipleOutputs;

    private static String baseOutputPath = "failure";

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<KeyValueListWritable> values, Context context)
            throws IOException, InterruptedException {

        boolean table1Exists = false;
        boolean table2Exists = false;
        Map<byte[], Map<byte[], Put>> mapHbaseComparePutMap = new HashMap<byte[], Map<byte[], Put>>();

        for (KeyValueListWritable hbaseCompareKeyValueList : values) {
            byte[] rowKey = hbaseCompareKeyValueList.getRowKey().copyBytes();
            if (hbaseCompareKeyValueList.getTableName().toString().equals(new Text(table1Name).toString())) {
                for (KeyValue keyValue : hbaseCompareKeyValueList.getKeyValueList()) {
                    addKeyValueToPut(mapHbaseComparePutMap, rowKey, keyValue, table1Name);

                }
                table1Exists = true;
            } else if (hbaseCompareKeyValueList.getTableName().toString().equals(new Text(table2Name).toString())) {
                for (KeyValue keyValue : hbaseCompareKeyValueList.getKeyValueList()) {
                    addKeyValueToPut(mapHbaseComparePutMap, rowKey, keyValue, table2Name);
                }
                table2Exists = true;
            }
        }

        if (!table1Exists) {
            multipleOutputs.write(new Text(table1Name), new Text(key.get()), "missing_rowkeys");
        } else if (!table2Exists) {
            multipleOutputs.write(new Text(table2Name), new Text(key.get()), "missing_rowkeys");
        } else {

            NavigableMap<byte[], List<Cell>> tableColumns1 = getFamilyCellMap(table1Name, mapHbaseComparePutMap);
            NavigableMap<byte[], List<Cell>> tableColumns2 = getFamilyCellMap(table2Name, mapHbaseComparePutMap);

            Set<byte[]> colFmly1 = tableColumns1.keySet();
            Set<byte[]> colFmly2 = tableColumns2.keySet();
            boolean matches = colFmly1.containsAll(colFmly2) && colFmly2.containsAll(colFmly1);
            if (!matches) {
                printColumnFamilyDetails(key, colFmly1, colFmly2);
            } else {
                for (byte[] bs : colFmly1) {

                    List<Cell> table1Cells = tableColumns1.get(bs);
                    Map<String, Cell> table1CellsQualifier = getTableCellsQualifier(table1Cells, key.get());
                    List<Cell> table2Cells = tableColumns2.get(bs);
                    Map<String, Cell> table2CellsQualifier = getTableCellsQualifier(table2Cells, key.get());
                    String tables = Bytes.toString(table1Name) + "|" + Bytes.toString(table2Name);

                    Set<String> table1CellsQualifierSet = table1CellsQualifier.keySet();
                    Set<String> table2CellsQualifierSet = table2CellsQualifier.keySet();
                    matches = table1CellsQualifierSet.containsAll(table2CellsQualifierSet)
                            && table2CellsQualifierSet.containsAll(table1CellsQualifierSet);

                    if (!matches) {
                        printColumnCellDetails(key, bs, tables, table1CellsQualifierSet, table2CellsQualifierSet);
                        break;
                    } else {
                        for (String table1CellsQualifierStr : table1CellsQualifierSet) {
                            Cell cell1 = table1CellsQualifier.get(table1CellsQualifierStr);
                            Cell cell2 = table2CellsQualifier.get(table1CellsQualifierStr);

                            if (!Arrays.equals(cell1.getValue(), cell2.getValue())) {
                                printColumnValueDetails(key, tables, cell1, cell2);
                            }
                        }
                    }
                }

            }
        }
    }

    /**
     * Method to print mismatching column values
     * 
     * @param key
     * @param tables
     * @param cell1
     * @param cell2
     * @throws IOException
     * @throws InterruptedException
     */
    private void printColumnValueDetails(ImmutableBytesWritable key, String tables, Cell cell1, Cell cell2)
            throws IOException, InterruptedException {

        StringBuffer headStr = new StringBuffer("Column values not matching ==> Rowkey [").append(
                Bytes.toString(key.get())).append("]");
        multipleOutputs.write(new Text(StringUtils.repeat("-", tables.length())),
                new Text(StringUtils.repeat("-", headStr.length())), baseOutputPath);
        multipleOutputs.write(new Text(tables), new Text(headStr.toString()), baseOutputPath);
        multipleOutputs.write(new Text(StringUtils.repeat("-", tables.length())),
                new Text(StringUtils.repeat("-", headStr.toString().length())), baseOutputPath);
        StringBuffer cell1Val = new StringBuffer("[").append(Bytes.toString(cell1.getFamily())).append("] :: ")
                .append(Bytes.toString(cell1.getQualifier())).append("=").append(Bytes.toString(cell1.getValue()));

        StringBuffer cell2Val = new StringBuffer("[").append(Bytes.toString(cell2.getFamily())).append("] :: ")
                .append(Bytes.toString(cell2.getQualifier())).append("=").append(Bytes.toString(cell2.getValue()));
        multipleOutputs.write(new Text(String.format("%-" + tables.length() + "s", Bytes.toString(table1Name))),
                new Text(cell1Val.toString()), baseOutputPath);
        multipleOutputs.write(new Text(String.format("%-" + tables.length() + "s", Bytes.toString(table2Name))),
                new Text(cell2Val.toString()), baseOutputPath);
    }

    /**
     * Method to print mismatching column cells
     * 
     * @param key
     * @param bs
     * @param tables
     * @param table1CellsQualifierSet
     * @param table2CellsQualifierSet
     * @throws IOException
     * @throws InterruptedException
     */
    private void printColumnCellDetails(ImmutableBytesWritable key, byte[] bs, String tables,
            Set<String> table1CellsQualifierSet, Set<String> table2CellsQualifierSet) throws IOException,
            InterruptedException {

        String unmatchedCellsInTable1 = subtract(table2CellsQualifierSet, table1CellsQualifierSet);
        String unmatchedCellsInTable2 = subtract(table1CellsQualifierSet, table2CellsQualifierSet);
        String headStr = "Column Cells Missing/Version Mismatch for column family " + Bytes.toString(bs)
                + " ==> Rowkey : " + Bytes.toString(key.get());
        multipleOutputs.write(new Text(StringUtils.repeat("-", tables.length())),
                new Text(StringUtils.repeat("-", headStr.length())), baseOutputPath);
        multipleOutputs.write(new Text(tables), new Text(headStr), baseOutputPath);
        multipleOutputs.write(new Text(StringUtils.repeat("-", tables.length())),
                new Text(StringUtils.repeat("-", headStr.length())), baseOutputPath);
        multipleOutputs.write(new Text(String.format("%-" + tables.length() + "s", Bytes.toString(table1Name))),
                new Text(unmatchedCellsInTable1), baseOutputPath);
        multipleOutputs.write(new Text(String.format("%-" + tables.length() + "s", Bytes.toString(table2Name))),
                new Text(unmatchedCellsInTable2), baseOutputPath);
    }

    /**
     * Method to print mismatching column families
     * 
     * @param key
     * @param colFmly1
     * @param colFmly2
     * @throws IOException
     * @throws InterruptedException
     */
    private void printColumnFamilyDetails(ImmutableBytesWritable key, Set<byte[]> colFmly1, Set<byte[]> colFmly2)
            throws IOException, InterruptedException {

        String tables = Bytes.toString(table1Name) + "|" + Bytes.toString(table2Name);
        String colFmlyMsg = Bytes.toString(table1Name) + "[";
        int index = 1;
        for (byte[] bs : colFmly1) {
            colFmlyMsg += Bytes.toString(bs);
            colFmlyMsg += (index < colFmly1.size()) ? "," : "]";
            index++;
        }
        colFmlyMsg += " | ";
        colFmlyMsg += Bytes.toString(table2Name) + "[";
        index = 1;
        for (byte[] bs : colFmly2) {
            colFmlyMsg += Bytes.toString(bs);
            colFmlyMsg += (index < colFmly2.size()) ? "," : "]";
            index++;
        }

        String message = "Column Families Not Matching ==> " + new Text(key.get()) + " :: " + colFmlyMsg;
        multipleOutputs.write(new Text(tables), new Text(message), baseOutputPath);
    }

    /**
     * Method to get table cell names
     * @param tableCells
     * @param rowkey
     * @return
     */
    private Map<String, Cell> getTableCellsQualifier(List<Cell> tableCells, byte[] rowkey) {

        Map<String, Cell> tableCellsQualifier = new HashMap<String, Cell>();
        StringBuffer qualifierStr = null;
        for (Cell tableCell : tableCells) {
            qualifierStr = new StringBuffer();
            qualifierStr = qualifierStr.append(Bytes.toString(rowkey)).append("||")
                    .append(Bytes.toString(tableCell.getFamily())).append("||")
                    .append(Bytes.toString(tableCell.getQualifier())).append("||").append(tableCell.getTimestamp())
                    .append("||").append(tableCell.getTypeByte()).append("||").append(tableCell.getMvccVersion());
            tableCellsQualifier.put(qualifierStr.toString(), tableCell);
        }
        return tableCellsQualifier;
    }

    /**
     * Method to retrieve difference between cells of two tables
     * @param table1QualifierStr
     * @param table2QualifierStr
     * @return
     */
    private String subtract(Set<String> table1QualifierStr, Set<String> table2QualifierStr) {

        List<String> unmatchedKeys = ListUtils.subtract(Arrays.asList(table1QualifierStr.toArray()),
                Arrays.asList(table2QualifierStr.toArray()));
        if (unmatchedKeys.isEmpty()) {
            return "";
        }
        List<String> unmatchedColumns = new ArrayList<String>();
        for (String string : unmatchedKeys) {
            unmatchedColumns.add(string.split("\\|\\|")[2]);
        }
        return unmatchedColumns.toString();
    }

    /**
     * Method to get map of table cells
     * @param tableName
     * @param mapHbaseComparePutMap
     * @return
     */
    private NavigableMap<byte[], List<Cell>> getFamilyCellMap(byte[] tableName,
            Map<byte[], Map<byte[], Put>> mapHbaseComparePutMap) {

        NavigableMap<byte[], List<Cell>> allTableColumns = null;
        Map<byte[], Put> hbaseComparePutMap = mapHbaseComparePutMap.get(tableName);
        Collection<Put> hbaseComparePutList = hbaseComparePutMap.values();
        for (Put put : hbaseComparePutList) {
            NavigableMap<byte[], List<Cell>> tableColumns = put.getFamilyCellMap();
            if (allTableColumns == null) {
                allTableColumns = tableColumns;
            } else {
                Set<byte[]> tableColumnsQual = tableColumns.keySet();
                for (byte[] bs : tableColumnsQual) {
                    List<Cell> tableColumnsCells = tableColumns.get(bs);
                    List<Cell> allTableColumnsCells = allTableColumns.get(bs);
                    allTableColumnsCells.addAll(tableColumnsCells);
                    allTableColumns.put(bs, allTableColumnsCells);
                }
            }
        }
        return allTableColumns;
    }

    /**
     * Method to add keyvalue to put object
     * @param mapHbaseComparePutMap
     * @param rowKey
     * @param keyValue
     * @param tableName
     * @throws IOException
     */
    private void addKeyValueToPut(Map<byte[], Map<byte[], Put>> mapHbaseComparePutMap, byte[] rowKey, Cell keyValue,
            byte[] tableName) throws IOException {

        Map<byte[], Put> hbaseComparePutMap = null;
        Put hbaseComparePut = null;
        if (mapHbaseComparePutMap.containsKey(tableName)) {
            hbaseComparePutMap = mapHbaseComparePutMap.get(tableName);
            if (hbaseComparePutMap.containsKey(rowKey)) {
                hbaseComparePut = hbaseComparePutMap.get(rowKey);
            } else {
                hbaseComparePut = new Put(rowKey);
            }
            hbaseComparePut.add(keyValue);
            hbaseComparePutMap.put(rowKey, hbaseComparePut);
        } else {
            hbaseComparePutMap = new HashMap<byte[], Put>();
            hbaseComparePut = new Put(rowKey);
            hbaseComparePut.add(keyValue);
            hbaseComparePutMap.put(rowKey, hbaseComparePut);
            mapHbaseComparePutMap.put(tableName, hbaseComparePutMap);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        table1Name = Bytes.toBytes(context.getConfiguration().get("table1"));
        table2Name = Bytes.toBytes(context.getConfiguration().get("table2"));
        multipleOutputs = new MultipleOutputs(context);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException,
            InterruptedException {

        multipleOutputs.close();
    }

}
