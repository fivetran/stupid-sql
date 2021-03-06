package com.fivetran.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class ToSqlFromMap implements ToSql<Map<String,Object>> {

    private final String[] keys, typeNames;
    private final int[] typeIds;

    public ToSqlFromMap(ResultSetMetaData metaData) throws SQLException {
        int n = metaData.getColumnCount();
        keys = new String[n];
        typeIds = new int[n];
        typeNames = new String[n];

        for (int i = 0; i < n; i++) {
            keys[i] = metaData.getColumnName(i + 1);
            typeIds[i] = metaData.getColumnType(i + 1);
            typeNames[i] = metaData.getColumnTypeName(i + 1);
        }
    }

    @Override
    public void populate(Map<String, Object> values, PreparedStatement query) throws SQLException {
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            int typeId = typeIds[i];
            String typeName = typeNames[i];

            if (!values.containsKey(key))
                query.setObject(i + 1, null);
            else
                query.setObject(i + 1, Coerce.javaToSql(query.getConnection(), values.get(key), typeId, typeName));
        }
    }
}
