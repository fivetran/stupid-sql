package com.fivetran.sql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Sql {
    private static final Logger log = LoggerFactory.getLogger(Sql.class);
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private final DataSource database;

    public Sql(DataSource database) {
        this.database = database;
    }

    // TODO transaction()

    /**
     * Create a {@code Query} which can be executed to produce a Stream<Row>.
     * <pre>
     * {@code
     * try (Stream<Row> rows = query("SELECT * FROM table WHERE id = ?", Row.class).execute(id)) {
     *     return rows.toArray(Row[]::new);
     * }
     * }</pre>
     *
     * @param sql
     * @param type
     * @return
     * @throws SQLException
     */
    public <T> Query<T> query(@Language("SQL") String sql, Class<T> type) throws SQLException {
        ResultSetMetaData schema = metaData(sql);
        ToJava<T> coerce = toJava(schema, type);

        return parameters -> {
            Connection connection = open(database);
            PreparedStatement query = connection.prepareStatement(sql);
            populate(connection, query, parameters);

            return stream(connection, query, coerce::coerce);
        };
    }

    private Cache<String, ResultSetMetaData> metaDataCache = CacheBuilder.newBuilder()
                                                                         .weakKeys()
                                                                         .build();

    private ResultSetMetaData metaData(String sql) throws SQLException {
        try {
            return metaDataCache.get(sql, () -> {
                try (Connection connection = open(database);
                     PreparedStatement statement = connection.prepareStatement(sql)) {

                    return statement.getMetaData();
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof SQLException)
                throw (SQLException) cause;
            else
                throw new RuntimeException(e);
        }
    }

    /**
     * @param sql
     * @return
     * @throws SQLException
     */
    public Statement statement(@Language("SQL") String sql) throws SQLException {
        return new Statement() {
            @Override
            public boolean execute(Object... parameters) throws SQLException {
                try (Connection connection = open(database);
                     PreparedStatement query = connection.prepareStatement(sql)) {

                    populate(connection, query, parameters);

                    return query.execute();
                }
            }

            @Override
            public Batch batch() throws SQLException {
                Connection connection = open(database);
                PreparedStatement query = connection.prepareStatement(sql);

                return new Batch(connection, query);
            }

            @Override
            public <K> Query<K> returnGeneratedKeys(Class<K> keyType) {
                return parameters -> {
                    Connection connection = open(database);
                    PreparedStatement query = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
                    populate(connection, query, parameters);
                    query.execute();

                    return stream(connection, query, query.getGeneratedKeys(), row -> (K) Coerce.sqlToJava(row, 1, keyType));
                };
            }
        };
    }

    private <T> ToJava<T> toJava(ResultSetMetaData schema, Class<T> type) throws SQLException {
        if (type == Map.class)
            return (ToJava<T>) new ToJavaMap(schema);
        else
            return new ToJavaPojo<T>(schema, type);
    }

    private <T> Stream<T> stream(Connection connection,
                                 PreparedStatement statement,
                                 final RowFunction<T> rowFunction) throws SQLException {
        return stream(connection, statement, statement.executeQuery(), rowFunction);
    }

    private <T> Stream<T> stream(Connection connection,
                                 PreparedStatement statement,
                                 ResultSet resultSet,
                                 final RowFunction<T> rowFunction) {
        Iterator<T> it = new Iterator<T>() {
            /**
             * true if we know there is a next row
             */
            boolean waiting = false;
            /**
             * true if we know there isn't a next row
             */
            boolean empty = false;

            @Override
            public boolean hasNext() {
                try {
                    // If stream is empty, return false
                    if (empty)
                        return false;
                        // If there is a waiting element in resultSet, return true
                    else if (waiting)
                        return true;
                        // If we succeed in advancing the cursor, note there is a waiting element
                    else if (resultSet.next()) {
                        waiting = true;

                        return true;
                    }
                    // If we fail to advance the cursor, note the stream is empty
                    else {
                        empty = true;

                        return false;
                    }
                } catch (SQLException e) {
                    empty = true;

                    throw new RuntimeException(e);
                }
            }

            @Override
            public T next() {
                try {
                    waiting = false;

                    return rowFunction.apply(resultSet);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Stream<T> rows = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);

        return new CloseableStream<>(rows, () -> {
            statement.close();
            connection.close();
        });
    }

    private void populate(Connection connection, PreparedStatement q, Object... parameters) throws SQLException {
        ParameterMetaData metaData = q.getParameterMetaData();

        if (metaData.getParameterCount() != parameters.length)
            throw new SqlMappingException("Expected " + metaData.getParameterCount() + " parameters but found " + parameters.length);

        for (int i = 0; i < parameters.length; i++) {
            String name = metaData.getParameterTypeName(i + 1);
            Object value = Coerce.javaToSql(connection, parameters[i], name);

            q.setObject(i + 1, value);
        }
    }

    /**
     * If debug logging is enabled, check if the returned connection has been closed after 1 second
     *
     * @param source
     * @return source.getConnection()
     * @throws SQLException
     */
    private static Connection open(DataSource source) throws SQLException {
        Connection connection = source.getConnection();

        if (log.isDebugEnabled()) {
            Throwable created = new Throwable();

            executor.schedule(() -> {
                try {
                    if (!connection.isClosed()) {
                        StackTraceElement[] stackTrace = created.getStackTrace();
                        StackTraceElement[] parentTrace = Arrays.copyOfRange(stackTrace, 1, stackTrace.length);
                        log.debug("Connection " + connection + " was created but never closed", new UnclosedConnection(parentTrace));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }, 1, TimeUnit.SECONDS);
        }

        return connection;
    }

    @FunctionalInterface
    public interface Query<T> {
        Stream<T> execute(Object... parameters) throws SQLException;
    }

    public interface Statement {
        boolean execute(Object... parameters) throws SQLException;

        Batch batch() throws SQLException;

        <K> Query<K> returnGeneratedKeys(Class<K> keyType);
    }

    @FunctionalInterface
    public interface RowFunction<T> {
        public T apply(ResultSet row) throws SQLException;
    }

    public class Batch implements AutoCloseable {
        private final Connection connection;
        private final PreparedStatement statement;

        public Batch(Connection connection, PreparedStatement statement) {
            this.connection = connection;
            this.statement = statement;
        }

        public void add(Object... parameters) throws SQLException {
            populate(connection, statement, parameters);
            statement.addBatch();
        }

        public int[] execute() throws SQLException {
            return statement.executeBatch();
        }

        @Override
        public void close() throws SQLException {
            statement.close();
            connection.close();
        }
    }
}
