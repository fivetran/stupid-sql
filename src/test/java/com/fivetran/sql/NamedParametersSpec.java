package com.fivetran.sql;

import com.fivetran.sql.named.NamedParameters;
import com.fivetran.sql.named.ParsedSql;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class NamedParametersSpec {
    @Test
    public void replaceNamed() {
        @Language("SQL")
        String sql = "SELECT * FROM t WHERE a = :value AND b = :value";
        ParsedSql parsed = NamedParameters.parseSqlStatement(sql);
        ImmutableMap<String, Integer> parameterValues = ImmutableMap.of("value", 1);
        String replaced = NamedParameters.substituteNamedParameters(parsed, parameterValues);
        Object[] values = NamedParameters.buildValueArray(parsed, parameterValues, null);

        assertEquals("SELECT * FROM t WHERE a = ? AND b = ?", replaced);
        assertArrayEquals(new Object[]{1, 1}, values);
    }

    @Test
    public void replaceCurlyBraceNamed() {
        @Language("SQL")
        String sql = "SELECT * FROM t WHERE a = :{value} AND b = :{value}";
        ParsedSql parsed = NamedParameters.parseSqlStatement(sql);
        ImmutableMap<String, Integer> parameterValues = ImmutableMap.of("value", 1);
        String replaced = NamedParameters.substituteNamedParameters(parsed, parameterValues);
        Object[] values = NamedParameters.buildValueArray(parsed, parameterValues, null);

        assertEquals("SELECT * FROM t WHERE a = ? AND b = ?", replaced);
        assertArrayEquals(new Object[]{1, 1}, values);
    }

    @Test
    public void dontReplaceStrings() {
        @Language("SQL")
        String sql = "SELECT * FROM t WHERE a = :value AND b = ':value'";
        ParsedSql parsed = NamedParameters.parseSqlStatement(sql);
        ImmutableMap<String, Integer> parameterValues = ImmutableMap.of("value", 1);
        String replaced = NamedParameters.substituteNamedParameters(parsed, parameterValues);
        Object[] values = NamedParameters.buildValueArray(parsed, parameterValues, null);

        assertEquals("SELECT * FROM t WHERE a = ? AND b = ':value'", replaced);
        assertArrayEquals(new Object[]{1}, values);
    }

    @Test
    public void dontReplacePostgresTypeAnnotations() {
        @Language("SQL")
        String sql = "SELECT * FROM t WHERE a = :value AND b = 'foo'::value";
        ParsedSql parsed = NamedParameters.parseSqlStatement(sql);
        ImmutableMap<String, Integer> parameterValues = ImmutableMap.of("value", 1);
        String replaced = NamedParameters.substituteNamedParameters(parsed, parameterValues);
        Object[] values = NamedParameters.buildValueArray(parsed, parameterValues, null);

        assertEquals("SELECT * FROM t WHERE a = ? AND b = 'foo'::value", replaced);
        assertArrayEquals(new Object[]{1}, values);
    }

}
