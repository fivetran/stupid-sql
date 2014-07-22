package sql;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ToJava<T> {
    T coerce(ResultSet row) throws SQLException;
}
