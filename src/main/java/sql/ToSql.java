package sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface ToSql<T> {
    void populate(T value, PreparedStatement query) throws SQLException;
}
