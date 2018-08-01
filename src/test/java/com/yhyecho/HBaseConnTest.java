package com.yhyecho;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by Echo on 7/16/18.
 */
public class HBaseConnTest {

    @Test
    public void testGetHBaseConn() throws Exception {
        Connection conn = HBaseConn.getHBaseConn();
        System.out.println(conn.isClosed());

        HBaseConn.closeConn();
        System.out.println(conn.isClosed());
    }

    @Test
    public void testGetTable() throws Exception {
        try {
            Table table = HBaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}