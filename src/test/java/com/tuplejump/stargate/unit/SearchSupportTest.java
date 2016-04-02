package com.tuplejump.stargate.unit;

import com.tuplejump.stargate.IndexContainer;
import com.tuplejump.stargate.RowIndex;
import com.tuplejump.stargate.cassandra.IndexTestBase;
import com.tuplejump.stargate.cassandra.RowIndexSupport;
import com.tuplejump.stargate.util.CQLUnitD;
import com.tuplejump.stargate.util.Record;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;


public class SearchSupportTest extends IndexTestBase {


    public SearchSupportTest() {
        cassandraCQLUnit = CQLUnitD.getCQLUnit(null);
    }

    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();

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

    @Test
    public void searchSupportTest() {
        setup();
        SecondaryIndexManager indexManager = Keyspace.open("keyspace1").getColumnFamilyStore("tag2").indexManager;
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("keyspace1", "tag2");
        RowIndex ri = (RowIndex) indexManager.getIndexes().iterator().next();

        RowIndexSupport support = ri.rowIndexSupport;
        //  IndexContainer container = support.indexContainer;
        // container.search()
        try {


        } finally

        {
            dropTable("keyspace1", "tag2");
            dropKS("keyspace1");
        }


    }
}
