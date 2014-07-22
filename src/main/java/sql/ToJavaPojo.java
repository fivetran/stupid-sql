package sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class ToJavaPojo<T> implements ToJava<T> {
    private final Constructor<T> constructor;
    private final Type[] types;
    private final int[] columnIndex;

    public ToJavaPojo(ResultSetMetaData metaData, Class<T> javaType) throws SQLException {
        constructor = (Constructor<T>) Arrays.stream(javaType.getConstructors())
                                             .filter(ToJavaPojo::isAnnotated)
                                             .findFirst()
                                             .orElseThrow(() -> new SqlMappingException(
                                                     "Can't find constructor with full @SqlAttribute annotations"));
        types = constructor.getGenericParameterTypes();
        String[] names = Arrays.stream(constructor.getParameterAnnotations())
                               .map(ToJavaPojo::findAttribute)
                               .map(ToJavaPojo::getName)
                               .toArray(String[]::new);
        columnIndex = findColumns(metaData, names);
    }

    private static int[] findColumns(ResultSetMetaData metaData, String[] names) throws SQLException {
        int[] columnIndex = new int[names.length];

        // Find index of each column name in SQL query
        for (int iColumn = 1; iColumn <= metaData.getColumnCount(); iColumn++) {
            String name = metaData.getColumnName(iColumn);

            for (int iName = 0; iName < names.length; iName++) {
                if (names[iName].equals(name))
                    columnIndex[iName] = iColumn;
            }
        }

        // Check that we have found every name
        for (int i = 0; i < names.length; i++) {
            if (columnIndex[i] == 0)
                throw new SqlMappingException("Missing column " + names[i] + " in query " + metaData);
        }

        return columnIndex;
    }

    private static boolean isAnnotated(Constructor<?> constructor) {
        Annotation[][] as = constructor.getParameterAnnotations();

        for (Annotation[] a : as) {
            if (findAttribute(a) == null)
                return false;
        }

        return true;
    }

    private static Annotation findAttribute(Annotation[] annotations) {
        for (Annotation a : annotations) {
            if (a.annotationType() == SqlAttribute.class)
                return a;
        }

        return null;
    }

    private static String getName(Annotation annotation) {
        return ((SqlAttribute) annotation).value();
    }

    public T coerce(ResultSet row) throws SQLException {
        int n = types.length;
        Object[] values = new Object[n];

        for (int i = 0; i < n; i++)
            values[i] = Coerce.sqlToJava(row, columnIndex[i], types[i]);

        try {
            return constructor.newInstance(values);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
