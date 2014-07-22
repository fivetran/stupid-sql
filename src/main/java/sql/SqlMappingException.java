package sql;

import java.sql.SQLException;

public class SqlMappingException extends SQLException {
    public SqlMappingException(String message) {
        super(message);
    }

    public SqlMappingException(Exception cause) {
        super(cause);
    }
}
