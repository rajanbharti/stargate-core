package com.tuplejump.stargate.unit;

import com.datastax.driver.core.UserType;
import com.tuplejump.stargate.IndexContainer;
import com.tuplejump.stargate.MonolithIndexContainer;
import com.tuplejump.stargate.RowIndex;
import com.tuplejump.stargate.Stargate;
import com.tuplejump.stargate.cassandra.IndexTestBase;
import com.tuplejump.stargate.cassandra.RowIndexSupport;
import com.tuplejump.stargate.cassandra.TableMapper;
import com.tuplejump.stargate.lucene.*;
import com.tuplejump.stargate.util.CQLUnitD;
import com.tuplejump.stargate.util.Record;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.index.SecondaryIndexManager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;
import org.apache.lucene.document.*;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;

public class RowIndexSupportTest extends IndexTestBase {

    public RowIndexSupportTest() {
        cassandraCQLUnit = CQLUnitD.getCQLUnit(null);
    }

    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();

    public static ByteBuffer str_to_bb(String msg) {
        try {
            return encoder.encode(CharBuffer.wrap(msg));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setup() {
        createKS("keyspace1");
        getSession().execute("USE keyspace1;");
        getSession().execute("CREATE TABLE TAG2(key int, tags text, state varchar, segment int, magic text, PRIMARY KEY(segment, key))");
        String options = "{\n" +
                "\t\"numShards\":1024,\n" +
                "\t\"metaColumn\":true,\n" +
                "\t\"fields\":{\n" +
                "\t\t\"tags\":{\"type\":\"text\"},\n" +
                "\t\t\"state\":{\"striped\":\"also\",\"analyzer\":\"org.apache.lucene.analysis.core.KeywordAnalyzer\"}\n" +
                "\t}\n" +
                "}\n";
        getSession().execute("CREATE CUSTOM INDEX tagsandstate ON TAG2(magic) USING 'com.tuplejump.stargate.RowIndex' WITH options ={'sg_options':'" + options + "'}");


    }

    public void insert() {
        String[] fields = {"key", "tags", "state", "segment"};
        String[] fieldTypes = {"int", "text", "varchar", "int", "text"};
        int i = 0;
        while (i < 20) {
            Record r1 = new Record(fields, new Object[]{(i + 1), "hello1 tag1 lol1", "CA", i}, fieldTypes);
            Record r2 = new Record(fields, new Object[]{(i + 2), "hello1 tag1 lol2", "LA", i}, fieldTypes);
            Record r3 = new Record(fields, new Object[]{(i + 3), "hello1 tag2 lol1", "NY", i}, fieldTypes);
            Record r4 = new Record(fields, new Object[]{(i + 4), "hello1 tag2 lol2", "TX", i}, fieldTypes);
            Record r5 = new Record(fields, new Object[]{(i + 5), "hllo3 tag3 lol3", "TX", i}, fieldTypes);
            Record r6 = new Record(fields, new Object[]{(i + 6), "hello2 tag1 lol1", "CA", i}, fieldTypes);
            Record r7 = new Record(fields, new Object[]{(i + 7), "hello2 tag1 lol2", "NY", i}, fieldTypes);
            Record r8 = new Record(fields, new Object[]{(i + 8), "hello2 tag2 lol1", "CA", i}, fieldTypes);
            Record r9 = new Record(fields, new Object[]{(i + 9), "hello2 tag2 lol2", "TX", i}, fieldTypes);
            Record r10 = new Record(fields, new Object[]{(i + 10), "hllo3 tag3 lol3", "TX", i}, fieldTypes);
            List<Record> tempRecords = Arrays.asList(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10);
            insertRecords("keyspace1", "TAG2", tempRecords);
            i = i + 10;
        }
    }

    public void insert2() {
        String[] fields = {"key", "tags", "state", "segment"};
        String[] fieldTypes = {"int", "text", "varchar", "int", "text"};
        int i = 20;
        while (i < 40) {
            Record r1 = new Record(fields, new Object[]{(i + 1), "hello1 tag1 lol1", "CA", i}, fieldTypes);
            Record r2 = new Record(fields, new Object[]{(i + 2), "hello1 tag1 lol2", "LA", i}, fieldTypes);
            Record r3 = new Record(fields, new Object[]{(i + 3), "hello1 tag2 lol1", "NY", i}, fieldTypes);
            Record r4 = new Record(fields, new Object[]{(i + 4), "hello1 tag2 lol2", "TX", i}, fieldTypes);
            Record r5 = new Record(fields, new Object[]{(i + 5), "hllo3 tag3 lol3", "TX", i}, fieldTypes);
            Record r6 = new Record(fields, new Object[]{(i + 6), "hello2 tag1 lol1", "CA", i}, fieldTypes);
            Record r7 = new Record(fields, new Object[]{(i + 7), "hello2 tag1 lol2", "NY", i}, fieldTypes);
            Record r8 = new Record(fields, new Object[]{(i + 8), "hello2 tag2 lol1", "CA", i}, fieldTypes);
            Record r9 = new Record(fields, new Object[]{(i + 9), "hello2 tag2 lol2", "TX", i}, fieldTypes);
            Record r10 = new Record(fields, new Object[]{(i + 10), "hllo3 tag3 lol3", "TX", i}, fieldTypes);
            List<Record> tempRecords = Arrays.asList(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10);
            insertRecords("keyspace1", "TAG2", tempRecords);
            i = i + 10;
        }
    }

    @Test
    public void rowIndexTest() {
        setup();

        SecondaryIndexManager indexManager = Keyspace.open("keyspace1").getColumnFamilyStore("tag2").indexManager;
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("keyspace1", "tag2");
        RowIndex ri = (RowIndex) indexManager.getIndexes().iterator().next();
        ByteBuffer rowKey = str_to_bb("magic");
        RowIndexSupport support = ri.rowIndexSupport;

        try {
            DecoratedKey key = support.tableMapper.decorateKey(rowKey);
            Indexer indexer = ri.indexContainer.indexer(key);
            Long oldCount = ri.indexContainer.indexer(key).approxRowCount();
            insert();
            try {
                Thread.sleep(100);                 //To give time to write data
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            Long newCount = ri.indexContainer.indexer(key).approxRowCount();
            Assert.assertEquals(20, newCount - oldCount);
        } finally {
            dropTable("keyspace1", "tag2");
            dropKS("keyspace1");
        }


    }

}
