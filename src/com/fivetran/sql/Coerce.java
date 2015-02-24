package com.fivetran.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.sun.javafx.scene.control.behavior.OptionalBoolean;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class Coerce {
    static Object sqlToJava(ResultSet row, int column, Type type) throws SQLException {
        // Primitive types
        if (type == boolean.class) {
            boolean result = row.getBoolean(column);

            if (row.wasNull())
                throw new NullPointerException("Can't encode " + column + "=null as boolean");
            else
                return result;
        } else if (type == int.class) {
            int result = row.getInt(column);

            if (row.wasNull())
                throw new NullPointerException("Can't encode " + column + "=null as int");
            else
                return result;
        } else if (type == long.class) {
            long result = row.getLong(column);

            if (row.wasNull())
                throw new NullPointerException("Can't encode " + column + "=null as long");
            else
                return result;
        } else if (type == double.class) {
            double result = row.getDouble(column);

            if (row.wasNull())
                throw new NullPointerException("Can't encode " + column + "=null as double");
            else
                return result;
        }
        // Reference types
        else if (type == Boolean.class)
            return (Boolean) row.getObject(column);
        else if (type == Integer.class)
            return (Integer) row.getObject(column);
        else if (type == Long.class)
            return (Long) row.getObject(column);
        else if (type == Double.class)
            return (Double) row.getObject(column);
        else if (type == String.class)
            return row.getString(column);
        else if (type == BigDecimal.class)
            return row.getBigDecimal(column);
        else if (type == Instant.class)
            if (row.getTimestamp(column) == null) return null;
            else return row.getTimestamp(column).toInstant();
        else if (type == LocalDate.class)
            return row.getDate(column).toLocalDate();
        // If column is json, delegate to Jackson
        else if (isJson(row, column))
            return sqlToJson(row, column, type);
        // Coerce arrays and json to list
        else if (isList(type))
            return sqlToList(row, column, listType(type));
        else if (type == OptionalBoolean.class) {
            Boolean value = (Boolean) row.getObject(column);

            if (value == null)
                return OptionalBoolean.ANY;
            else
                return value ? OptionalBoolean.TRUE : OptionalBoolean.FALSE;
        }
        else if (type == OptionalInt.class) {
            Integer value = (Integer) row.getObject(column);

            if (value == null)
                return OptionalInt.empty();
            else
                return OptionalInt.of(value);
        }
        else if (type == OptionalLong.class) {
            Long value = (Long) row.getObject(column);

            if (value == null)
                return OptionalLong.empty();
            else
                return OptionalLong.of(value);
        }
        else if (type == OptionalDouble.class) {
            Double value = (Double) row.getObject(column);

            if (value == null)
                return OptionalDouble.empty();
            else
                return OptionalDouble.of(value);
        }
        else if (isOptional(type))
            return sqlToOptional(row, column, optionalType(type));
        else
            throw new IllegalArgumentException("Don't know how to create " + type);
        // TODO arrays
    }

    private static Object sqlToOptional(ResultSet row, int column, Type type) throws SQLException {
        Object value = row.getObject(column);

        return Optional.ofNullable(value);
    }

    private static Type optionalType(Type type) {
        if (type == Optional.class)
            return Object.class;
        else if (type instanceof ParameterizedType)
            return ((ParameterizedType) type).getActualTypeArguments()[0];
        else
            throw new RuntimeException("Don't know how to get optional element type from " + type);
    }

    private static boolean isOptional(Type type) {
        if (type == Optional.class)
            return true;
        else if (type instanceof ParameterizedType)
            return ((ParameterizedType) type).getRawType() == Optional.class;
        else
            return false;
    }

    private static boolean isList(Type type) {
        if (type == List.class)
            return true;
        else if (type instanceof ParameterizedType)
            return ((ParameterizedType) type).getRawType() == List.class;
        else
            return false;
    }

    private static Type listType(Type type) {
        if (type == List.class)
            return Object.class;
        else if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == List.class)
            return ((ParameterizedType) type).getActualTypeArguments()[0];
        else
            throw new RuntimeException("Don't know how to get list element type from " + type);
    }

    // Convert array or json to List<T>
    private static List sqlToList(ResultSet row, int column, Type element) throws SQLException {
        ResultSetMetaData metaData = row.getMetaData();
        int columnType = metaData.getColumnType(column);

        // If column is an array, coerce each element
        if (columnType == java.sql.Types.ARRAY) {
            ResultSet array = row.getArray(column).getResultSet();
            ArrayList acc = Lists.newArrayList();

            while (array.next())
                acc.add(sqlToJava(array, 2, element));

            return acc;
        }
        else {
            String foundType = metaData.getColumnTypeName(column);
            String desiredType = "List<" + element.getTypeName() + ">";

            throw new SqlMappingException("Don't know how to convert " + foundType + " to " + desiredType);
        }
    }

    private static boolean isJson(ResultSet row, int column) throws SQLException {
        ResultSetMetaData metaData = row.getMetaData();
        int columnType = metaData.getColumnType(column);

        return columnType == Types.OTHER && metaData.getColumnTypeName(column).equals("json");
    }

    static class Resolved extends TypeReference<Void> {
        public final Type type;

        Resolved(Type type) {
            this.type = type;
        }

        @Override
        public Type getType() {
            return type;
        }
    }

    // Convert json to Map<String, T>
    private static Object sqlToJson(ResultSet row, int column, Type element) throws SQLException {
        String text = row.getString(column);

        try {
            if (text == null)
                return null;
            else
                return Config.JSON.readValue(text, new Resolved(element));
        } catch (IOException e) {
            throw new SqlMappingException(e);
        }
    }



    // TODO this should really use sql.Types integers, and fall back to strings
    public static Object javaToSql(Connection connection, Object value, int typeId, String type) throws SQLException {
        if (typeId == Types.TIMESTAMP || typeId == Types.TIMESTAMP_WITH_TIMEZONE)
            return javaToTimestamp(value);
        else if (typeId == Types.DATE)
            return javaToDate(value);
        else if (type.equals("json"))
            try {
                String serialized = Config.JSON.writeValueAsString(value); // TODO PGobject
                PGobject json = new PGobject();

                json.setType("json");
                json.setValue(serialized);

                return json;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        else if (type.charAt(0) == '_')
            return javaToArray(connection, value, typeId, type.substring(1));
        else
            return value;
    }

    private static Date javaToDate(Object value) throws SqlMappingException {
        if (value instanceof LocalDate)
            return Date.valueOf((LocalDate) value);
        else if (value instanceof Calendar)
            return new Date(((Calendar) value).getTimeInMillis());
        else
            throw new SqlMappingException("Don't know how to coerce " + value + " to date");
    }

    private static Timestamp javaToTimestamp(Object value) throws SqlMappingException {
        if (value instanceof Instant)
            return Timestamp.from((Instant) value);
        else if (value instanceof java.util.Date)
            return new Timestamp(((Date) value).getTime());
        else
            throw new SqlMappingException("Don't know how to coerce " + value + " to timestamp");
    }

    private static Array javaToArray(Connection connection, Object value, int typeId, String typeName) throws SQLException {
        if (value instanceof List)
            return listToArray(connection, (List) value, typeId, typeName);
        else if (value instanceof Object[])
            return arrayToArray(connection, (Object[]) value, typeId, typeName);
        else
            throw new SqlMappingException("Don't know how to coerce " + value + " to array");
    }

    private static Array arrayToArray(Connection connection, Object[] values, int typeId, String typeName) throws SQLException {
        Object[] array = new Object[values.length];

        for (int i = 0; i < values.length; i++)
            array[i] = javaToSql(connection, values[i], typeId, typeName);

        return connection.createArrayOf(typeName, array);
    }

    private static Array listToArray(Connection connection, List values, int typeId, String typeName) throws SQLException {
        Object[] array = new Object[values.size()];

        for (int i = 0; i < values.size(); i++)
            array[i] = javaToSql(connection, values.get(i), typeId, typeName);

        return connection.createArrayOf(typeName, array);
    }
}
