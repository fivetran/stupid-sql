package com.fivetran.sql;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class JsonSpecs {
    private Sql sql() {
        PGSimpleDataSource source = new PGSimpleDataSource();

        source.setDatabaseName("public");

        return new Sql(source);
    }

    @Test
    public void coerceJson() throws SQLException {
        try (Stream<Map> example = sql().query("SELECT '{\"a\":{\"b\":1}}'::JSON AS example", Map.class)
                                        .execute()) {
            JsonNodeFactory f = JsonNodeFactory.instance;
            Map expected = ImmutableMap.of("example", f.objectNode()
                                                       .set("a", f.objectNode()
                                                                  .set("b", f.numberNode(1))));
            Map actual = example.findFirst().get();

            assertEquals(expected, actual);
        }
    }
}
