package com.tuplejump.stargate.util;

import com.datastax.driver.core.Row;

import java.lang.reflect.Array;
import java.util.*;

public class Record {

    private Map recordDefinition = new HashMap<String, String>();
    private Map record = new HashMap<String, Object>();

    public Record(String[] field, Object[] values, String[] type) {
        if (field.length == values.length) {
            for (int i = 0; i < field.length; i++) {
                recordDefinition.put(field[i], type[i]);
              /*  if (type[i] == "int")
                    record.put(field[i], values[i]);
                else if (type[i] == "boolean")
                    record.put(field[i], values[i]);
                else*/
                record.put(field[i], values[i].toString());
            }
        }
    }

    Record(Map record) {
        this.record = record;
    }

    public Record(Row row) {
        List<String> fields = new ArrayList<>();
        //   List<String> types = new ArrayList<String>();
        row.getColumnDefinitions().iterator().forEachRemaining(c -> {
            fields.add(c.getName());
            //  types.add(c.getType().toString());
        });
        if (fields.contains("stargate"))
            fields.remove("stargate");
        fields.iterator().forEachRemaining(col -> {
            record.put(col, row.getObject(col).toString());
        });
    }

    public String getFieldsString() {
        Iterator it = record.entrySet().iterator();
        List<String> fieldList = new ArrayList<String>(record.keySet());
        return mkString(fieldList);
    }

    public String getValuesString() {
        Iterator it = record.entrySet().iterator();
        List<String> valueList = new ArrayList<String>(record.values());
        List<String> types = new ArrayList<String>(recordDefinition.values());
        return mkString(valueList, types);
    }

    private String mkString(List<String> list) {
        StringBuilder result = new StringBuilder();
        for (String s : list) {
            result.append(s);
            result.append(",");
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : "";
    }

    private String mkString(List<String> list, List<String> recordDefinition) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        for (Object s : list) {
            if (recordDefinition.get(i) == "int" || recordDefinition.get(i) == "boolean") {
                result.append(s.toString());
                result.append(",");
            } else {
                result.append("'" + s.toString() + "'");
                result.append(",");
            }
            i++;
        }
        return result.length() > 0 ? result.substring(0, result.length() - 1) : "";
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
