package com.tuplejump.stargate.cassandra;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.statements.MultiColumnRestriction;

import java.util.LinkedList;
import java.util.List;

public class Record extends IndexTestBase {
    private String keyspace;
    private static Record instance = new Record();

    private Record() {
    }

    public static Record getInstance() {
        return instance;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void insert(String table, String fields, String values) {
        getSession().execute("insert into " + keyspace + " " + table + "(" + fields + ") values(" + values + ");");
    }

    public boolean assertResult(ResultSet resultSet, List expected, String key) {
        List<Object> fetched = new LinkedList<Object>();
        resultSet.all().iterator().forEachRemaining(row -> {
            fetched.add(row.getObject(key));
        });
        return (expected.equals(fetched));
    }
}
