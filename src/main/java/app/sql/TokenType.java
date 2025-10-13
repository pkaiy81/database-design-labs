package app.sql;

public enum TokenType {
    IDENT, INT, STRING,
    STAR, COMMA, DOT, EQ, LPAREN, RPAREN,
    SELECT, FROM, WHERE, JOIN, ON, AND,
    EOF
}
