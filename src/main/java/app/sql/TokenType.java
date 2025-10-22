package app.sql;

public enum TokenType {
    IDENT, INT, STRING,
    STAR, COMMA, DOT, EQ, GT, LT, GE, LE, LPAREN, RPAREN,
    SELECT, DISTINCT, FROM, WHERE, JOIN, ON, AND,
    GROUP, HAVING, COUNT, SUM, AVG, MIN, MAX,
    INSERT, INTO, VALUES,
    UPDATE, SET,
    DELETE,
    ORDER, BY, LIMIT, ASC, DESC,
    EXPLAIN,
    CREATE, INDEX,
    USING, BTREE,
    BETWEEN, NOT,
    SYMBOL, // 未知のシンボル
    KEYWORD, // 未知のキーワード
    EOF
}
