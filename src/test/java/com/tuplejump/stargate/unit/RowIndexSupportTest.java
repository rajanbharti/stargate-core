package com.tuplejump.stargate.unit;

import com.google.common.collect.Iterators;
import com.tuplejump.stargate.MonolithIndexContainer;
import com.tuplejump.stargate.RowIndex;
import com.tuplejump.stargate.cassandra.IndexTestBase;
import com.tuplejump.stargate.cassandra.RowIndexSupport;
import com.tuplejump.stargate.lucene.*;
import com.tuplejump.stargate.util.CQLUnitD;
import com.tuplejump.stargate.util.Record;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RowIndexSupportTest extends IndexTestBase {

    @Mock
    BasicIndexer mockIndexer;

    public RowIndexSupportTest() {
        cassandraCQLUnit = CQLUnitD.getCQLUnit(null);
    }

    @Before
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
    public void rowIndexTest() {
        SecondaryIndexManager indexManager = Keyspace.open("keyspace1").getColumnFamilyStore("tag2").indexManager;
        RowIndex ri = (RowIndex) indexManager.getIndexes().iterator().next();
        RowIndexSupport support = ri.rowIndexSupport;
        try {

            MonolithIndexContainer container = (MonolithIndexContainer) ri.indexContainer;
            container.indexer = mockIndexer; //assigning a mock indexer to container
            insert();
            try {
                Thread.sleep(100);                 //To give time to write data
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            verify(mockIndexer, times(20)).upsert(any(), any());
            Term term = new Term(LuceneUtils.PK_INDEXED, "0:1\\:"); //(segment:key\:)
            Term term1 = new Term(LuceneUtils.PK_INDEXED, "0:10\\:");
            Term term3 = new Term(LuceneUtils.PK_INDEXED, "10:11\\:");
            Term term4 = new Term(LuceneUtils.PK_INDEXED, "10:20\\:");

            ArgumentCaptor<Iterable> arg = ArgumentCaptor.forClass(Iterable.class);
            verify(mockIndexer).upsert(eq(term), arg.capture());
            Iterable<Field> fields = arg.getValue();
            assertEquals(10, Iterators.size(fields.iterator()));
            verify(mockIndexer).upsert(eq(term1), any());
            verify(mockIndexer).upsert(eq(term3), any());
            verify(mockIndexer).upsert(eq(term4), any());

        } finally {
            dropTable("keyspace1", "tag2");
            dropKS("keyspace1");
        }
    }

}
