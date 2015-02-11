package com.fivetran.sql;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SqlSpecs {
    private Sql sql() {
        PGSimpleDataSource source = new PGSimpleDataSource();

        source.setDatabaseName("public");

        return new Sql(source);
    }

    private boolean isClosed = false;

    @Test
    public void streamGetsClosed() throws SQLException {
        // NOTE: findFirst() consumes the stream
        try (Stream<Map> one = sql().query("SELECT 1 AS one", Map.class)
                                    .execute()
                                    .onClose(() -> isClosed = true)) {

            assertEquals(one.findFirst().get(), ImmutableMap.of("one", 1));
        }

        assertTrue(isClosed);
    }
}
