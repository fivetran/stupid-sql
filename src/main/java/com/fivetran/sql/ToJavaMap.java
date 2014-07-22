package com.fivetran.sql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class ToJavaMap implements ToJava<Map> {

    private final ResultSetMetaData metaData;

    public ToJavaMap(ResultSetMetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public Map coerce(ResultSet row) throws SQLException {
        Map<String, Object> acc = Maps.newLinkedHashMap();

        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String key = metaData.getColumnName(i + 1);
            Object value = row.getObject(i + 1);

            acc.put(key, sqlToJava(value));
        }

        return acc;
    }

    private static Object sqlToJava(Object value) throws SQLException {
        if (value instanceof PGobject)
            return pgToJava((PGobject) value);
        else if (value instanceof Array)
            return sqlToArray((Array) value);
        else if (value instanceof Timestamp)
            return ((Timestamp) value).toInstant();
        else
            return value;
    }

    private static Object pgToJava(PGobject value) {
        if (value.getType().equals("json"))
            return readJsonUnsafe(value.getValue());
        else
            return value.getValue();
    }

    private static List sqlToArray(Array sql) throws SQLException {
        String type = sql.getBaseTypeName();
        ResultSet array = sql.getResultSet();

        if (type.equals("json"))
            return sqlToJsonArray(array);
        else
            return sqlToSimpleArray(array);
    }

    private static List sqlToJsonArray(ResultSet array) throws SQLException {
        List values = Lists.newArrayList();

        while (array.next()) {
            String text = array.getString(2);
            Object value = readJsonUnsafe(text);

            values.add(value);
        }

        return values;
    }

    // JSON from the database should always be well-formatted
    private static Object readJsonUnsafe(String text) {
        try {
            return Config.JSON.readTree(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List sqlToSimpleArray(ResultSet array) throws SQLException {
        List values = Lists.newArrayList();

        while (array.next()) {
            Object value = array.getObject(2);

            values.add(value);
        }

        return values;
    }
}
