package app.sql;

public enum TokenType {
    IDENT, INT, STRING,
    STAR, COMMA, DOT, EQ, LPAREN, RPAREN,
    SELECT, FROM, WHERE, JOIN, ON, AND,
    ORDER, BY, LIMIT, ASC, DESC,
    GROUP, COUNT, SUM, AVG, MIN, MAX,
    EOF
}
