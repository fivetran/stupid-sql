package com.fivetran.sql;

import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

public class DateSpecs {

    private Sql sql() {
        PGSimpleDataSource source = new PGSimpleDataSource();

        source.setDatabaseName("public");

        return new Sql(source);
    }

    @Test
    public void date() throws SQLException {
        try (ResultSet rows = sql().query("SELECT cast('2000-01-01' AS DATE) AS d").execute()) {
            rows.next();

            Object byIndex = rows.getObject(1);
            Object byName = rows.getObject("d");

            LocalDate expected = LocalDate.of(2000, 1, 1);

            assertEquals(expected, byIndex);
            assertEquals(expected, byName);
        }
    }

    @Test
    public void timestamp() throws SQLException {
        try (ResultSet rows = sql().query("SELECT cast('2000-01-01' AS TIMESTAMP) AS d").execute()) {
            rows.next();

            Object byIndex = rows.getObject(1);
            Object byName = rows.getObject("d");

            Instant expected = rows.getTimestamp(1).toInstant();

            assertEquals(expected, byIndex);
            assertEquals(expected, byName);
        }
    }

    @Test
    public void timestampWithTimezone() throws SQLException {
        try (ResultSet rows = sql().query("SELECT cast('2000-01-01T00:00:00.000Z' as TIMESTAMP WITH TIME ZONE) as d").execute()) {
            rows.next();

            Object byIndex = rows.getObject(1);
            Object byName = rows.getObject("d");

            LocalDateTime local = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
            Instant expected = local.toInstant(ZoneOffset.UTC);

            assertEquals(expected, byIndex);
            assertEquals(expected, byName);
        }
    }
}
