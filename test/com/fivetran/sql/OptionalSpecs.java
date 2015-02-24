package com.fivetran.sql;

import com.sun.javafx.scene.control.behavior.OptionalBoolean;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class OptionalSpecs {

    private Sql sql() {
        PGSimpleDataSource source = new PGSimpleDataSource();

        source.setDatabaseName("public");

        return new Sql(source);
    }

    public static class Optionals {
        public final Optional<String> strings;
        public final OptionalInt ints;
        public final OptionalLong longs;
        public final OptionalBoolean booleans;
        public final OptionalDouble doubles;

        public Optionals(@SqlAttribute("strings") Optional<String> strings,
                         @SqlAttribute("ints") OptionalInt ints,
                         @SqlAttribute("longs") OptionalLong longs,
                         @SqlAttribute("booleans") OptionalBoolean booleans,
                         @SqlAttribute("doubles") OptionalDouble doubles) {
            this.strings = strings;
            this.ints = ints;
            this.longs = longs;
            this.booleans = booleans;
            this.doubles = doubles;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Optionals optionals = (Optionals) o;

            if (booleans != optionals.booleans) return false;
            if (doubles != null ? !doubles.equals(optionals.doubles) : optionals.doubles != null) return false;
            if (ints != null ? !ints.equals(optionals.ints) : optionals.ints != null) return false;
            if (longs != null ? !longs.equals(optionals.longs) : optionals.longs != null) return false;
            if (strings != null ? !strings.equals(optionals.strings) : optionals.strings != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = strings != null ? strings.hashCode() : 0;
            result = 31 * result + (ints != null ? ints.hashCode() : 0);
            result = 31 * result + (longs != null ? longs.hashCode() : 0);
            result = 31 * result + (booleans != null ? booleans.hashCode() : 0);
            result = 31 * result + (doubles != null ? doubles.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Optionals{" +
                   "strings=" + strings +
                   ", ints=" + ints +
                   ", longs=" + longs +
                   ", booleans=" + booleans +
                   ", doubles=" + doubles +
                   '}';
        }
    }

    @Test
    public void some() throws SQLException {
        try (Stream<Optionals> found = sql().query("SELECT 'foo' as strings, 1 as ints, 1::bigint as longs, true as booleans, 1.0::double precision as doubles", Optionals.class)
                                            .execute()) {
            assertEquals(new Optionals(Optional.of("foo"), OptionalInt.of(1), OptionalLong.of(1), OptionalBoolean.TRUE, OptionalDouble.of(1.0)),
                         found.findFirst().get());
        }
    }

    @Test
    public void none() throws SQLException {
        try (Stream<Optionals> found = sql().query("SELECT null as strings, null as ints, null as longs, null as booleans, null as doubles", Optionals.class)
                                            .execute()) {
            assertEquals(new Optionals(Optional.<String>empty(), OptionalInt.empty(), OptionalLong.empty(), OptionalBoolean.ANY, OptionalDouble.empty()),
                         found.findFirst().get());
        }
    }
}
