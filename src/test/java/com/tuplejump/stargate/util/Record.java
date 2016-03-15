package com.tuplejump.stargate.util;

import com.datastax.driver.core.Row;

import java.util.*;

public class Record {

    private List<String> recordDefinition = new LinkedList<String>();

    private Map record = new HashMap<String, Object>();

    public Record(String field, String values, String def) {
        String[] fieldList = field.split(",");
        String[] valueList = values.split(",");
        String[] definition = def.split(",");
        if (fieldList.length == valueList.length) {
            for (int i = 0; i < fieldList.length; i++)
                if (definition[i] == "int")
                    record.put(fieldList[i], Integer.parseInt(valueList[i]));
                else
                    record.put(fieldList[i], valueList[i]);

        }
    }

    Record(Map record) {
        this.record = record;
    }

    public Record(Row row) {
        Map fetched = new HashMap<String, Object>();
        List columns = row.getColumnDefinitions().asList();
        columns.iterator().forEachRemaining(col -> {
            fetched.put(col, row.getObject(col.toString()));
        });
        record = fetched;
    }


    private String mkString(List<String> list) {
        StringBuilder result = new StringBuilder();
        for (String s : list) {
            result.append(s);
            result.append(",");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : "";
    }

    public String getFieldsString() {
        Map data = record;
        Iterator it = data.entrySet().iterator();
        List<String> fieldList = new ArrayList<String>(data.keySet());
        return mkString(fieldList);
    }

    public String getValuesString() {
        Map data = record;
        Iterator it = data.entrySet().iterator();
        List<String> valueList = new ArrayList<String>(data.values());
        return mkString(valueList);
    }

    public Map getRecord() {
        return record;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!Record.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final Record other = (Record) obj;
        if ((this.record == null) ? (other.record != null) : !this.record.equals(other.record)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }

    @Override
    public String toString() {
        return record.toString();
    }
}
