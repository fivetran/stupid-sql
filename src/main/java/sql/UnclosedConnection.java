package sql;

import java.sql.SQLException;

public class UnclosedConnection extends SQLException {
    public UnclosedConnection(StackTraceElement[] createdAt) {
        this.setStackTrace(createdAt);
    }
}
