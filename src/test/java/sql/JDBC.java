package sql;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class JDBC {

    // There is a bug in old versions of the postgres driver that allows PreparedStatement to become stale

    public static final String SELECT_1 = "SELECT 1 AS value";

    @Test
    public void freshPreparedStatement() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/testdb")) {
            PreparedStatement statement = connection.prepareStatement(SELECT_1);

            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            assertEquals(1, resultSet.getInt(1));

            ResultSetMetaData metaData = statement.getMetaData();
            assertEquals("value", metaData.getColumnName(1));
        }
    }

    @Test
    public void stalePreparedStatement() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/testdb")) {
            PreparedStatement statement = connection.prepareStatement(SELECT_1);

            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            assertEquals(1, resultSet.getInt(1));
            resultSet.close();

            ResultSetMetaData metaData = statement.getMetaData();
            assertEquals("value", metaData.getColumnName(1));
        }
    }
}
