/*
 * Copyright 2014, Tuplejump Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tuplejump.stargate.cassandra;

import com.tuplejump.stargate.util.CQLUnitD;
import com.tuplejump.stargate.util.Record;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: satya
 */
public class PartitionKeyIndexTest extends IndexTestBase {

    String keyspace = "dummyksPartKey";
    List<Record> records = new ArrayList<Record>();

    public PartitionKeyIndexTest() {
        cassandraCQLUnit = CQLUnitD.getCQLUnit(null);
    }

    @Test
    public void shouldIndexPerRow() throws Exception {
        //hack to always create new Index during testing
        try {
            createKS(keyspace);
            createTableAndIndexForRow();
            countResults("TAG2", "", false, true);
            Assert.assertEquals(12, countResults("TAG2", "magic = '" + q("state", "state:CA") + "'", true));
            Assert.assertEquals(12, countResults("TAG2", "magic = '" + q("tags", "tags:hello* AND state:CA") + "'", true));
            String q1 = "{ type:\"wildcard\", field:\"tags\", value:\"hello*\" }";
            String q2 = "{ type:\"match\", field:\"state\", value:\"CA\" }";
            Assert.assertEquals(12, countResults("TAG2", "magic = '" + bq(q1, q2) + "'", true));
            Assert.assertEquals(12, countResults("TAG2", "magic = '" + q("tags", "tags:hello? AND state:CA") + "'", true));
            Assert.assertEquals(8, countResults("TAG2", "magic = '" + q("tags", "tags:hello2 AND state:CA") + "'", true));
        } finally {
            dropTable(keyspace, "TAG2");
            dropKS(keyspace);
        }
    }

    private void createTableAndIndexForRow() {
        String options = "{\n" +
                "\t\"fields\":{\n" +
                "\t\t\"state\":{},\n" +
                "\t\t\"tags\":{}\n" +
                "\t}\n" +
                "}";

        String[] fields = {"key", "tags", "state"};
        String[] fieldTypes = {"int", "varchar", "varchar"};
        getSession().execute("USE " + keyspace + ";");
        getSession().execute("CREATE TABLE TAG2(key int, tags varchar, state varchar, magic text, PRIMARY KEY ((key,state)))");
        int i = 0;
        while (i < 40) {
            if (i == 20) {
                getSession().execute("CREATE CUSTOM INDEX tagsandstate ON TAG2(magic) USING 'com.tuplejump.stargate.RowIndex' WITH options ={'sg_options':'" + options + "'}");
            }
            Record r1 = new Record(fields, new Object[]{(i + 1), "hello1 tag1 lol1", "CA"}, fieldTypes);
            Record r2 = new Record(fields, new Object[]{(i + 1), "hello1 tag1 lol2", "LA"}, fieldTypes);
            Record r3 = new Record(fields, new Object[]{(i + 1), "hello1 tag2 lol1", "NY"}, fieldTypes);
            Record r4 = new Record(fields, new Object[]{(i + 1), "hello1 tag2 lol2", "TX"}, fieldTypes);
            Record r5 = new Record(fields, new Object[]{(i + 2), "hllo3 tag3 lol3", "TX"}, fieldTypes);
            Record r6 = new Record(fields, new Object[]{(i + 2), "hello2 tag1 lol1", "CA"}, fieldTypes);
            Record r7 = new Record(fields, new Object[]{(i + 2), "hello2 tag1 lol2", "NY"}, fieldTypes);
            Record r8 = new Record(fields, new Object[]{(i + 3), "hello2 tag2 lol1", "CA"}, fieldTypes);
            Record r9 = new Record(fields, new Object[]{(i + 3), "hello2 tag2 lol2", "TX"}, fieldTypes);
            Record r10 = new Record(fields, new Object[]{(i + 4), "hllo3 tag3 lol3", "TX"}, fieldTypes);
            List<Record> tempRecords = Arrays.asList(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10);
            insertRecords(keyspace, "TAG2", tempRecords);
            records.addAll(tempRecords);
            i = i + 10;
        }
    }
}
