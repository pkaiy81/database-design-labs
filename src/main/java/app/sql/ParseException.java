// src/main/java/app/sql/ParseException.java
package app.sql;

public class ParseException extends RuntimeException {
    public ParseException(String message) {
        super("Parse error: " + message);
    }
}
