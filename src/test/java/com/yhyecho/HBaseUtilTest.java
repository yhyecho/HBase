package com.yhyecho;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Echo on 7/17/18.
 */
public class HBaseUtilTest {

    @Test
    public void testCreateTable() throws Exception {
        HBaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"});
    }

    @Test
    public void testPutRow() throws Exception {
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "name", "file1.txt");
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "type", "txt");
        HBaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "size", "1024");
        HBaseUtil.putRow("FileTable", "rowkey1", "saveInfo", "creator", "echo");

        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "name", "file2.txt");
        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "type", "avi");
        HBaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "size", "2048");
        HBaseUtil.putRow("FileTable", "rowkey2", "saveInfo", "creator", "tim");
    }

    @Test
    public void testGetRow() throws Exception {
        Result result = HBaseUtil.getRow("FileTable", "rowkey1");
        if (result != null) {
            System.out.println("rowkey=" + Bytes.toString(result.getRow()));
            System.out.println("fileName" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
        }
    }

    @Test
    public void testGetScanner() throws Exception {
        ResultScanner scanner = HBaseUtil.getScanner("FileTable", "rowkey1", "rowkey2");
        if (scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
            });
            scanner.close();
        }
    }

    @Test
    public void testDeleteRow() throws Exception {
        HBaseUtil.deleteRow("FileTable", "rowkey1");
    }

    @Test
    public void testDeleteTable() throws Exception {
        HBaseUtil.deleteTable("FileTable");
    }
}