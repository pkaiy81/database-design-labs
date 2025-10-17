package app.sql;

import java.util.Locale;

public final class Lexer {
    private final String s;
    private int i = 0;
    private String text;
    private TokenType type;
    private String word;

    public boolean isWord(String s) {
        return word != null && word.equalsIgnoreCase(s);
    }

    public Lexer(String sql) {
        this.s = sql;
        next();
    }

    public TokenType type() {
        return type;
    }

    public String text() {
        return text;
    }

    public void next() {
        skipWs();
        if (i >= s.length()) {
            type = TokenType.EOF;
            text = "";
            return;
        }
        char c = s.charAt(i);
        switch (c) {
            case '*':
                i++;
                type = TokenType.STAR;
                text = "*";
                return;
            case ',':
                i++;
                type = TokenType.COMMA;
                text = ",";
                return;
            case '.':
                i++;
                type = TokenType.DOT;
                text = ".";
                return;
            case '=':
                i++;
                type = TokenType.EQ;
                text = "=";
                return;
            case '(':
                i++;
                type = TokenType.LPAREN;
                text = "(";
                return;
            case ')':
                i++;
                type = TokenType.RPAREN;
                text = ")";
                return;
            // 記号（>= <=）
            case '>':
                i++;
                if (i < s.length() && s.charAt(i) == '=') {
                    i++;
                    type = TokenType.GE;
                    text = ">=";
                    return;
                }
                type = TokenType.GT;
                text = ">";
                return;
            case '<':
                i++;
                if (i < s.length() && s.charAt(i) == '=') {
                    i++;
                    type = TokenType.LE;
                    text = "<=";
                    return;
                }
                type = TokenType.LT;
                text = "<";
                return;
            case '\'':
                readString();
                return;
        }
        if (Character.isDigit(c)) {
            readInt();
            return;
        }
        if (isIdentStart(c)) {
            readIdentOrKeyword();
            return;
        }
        throw error("unexpected char: " + c);
    }

    private void readInt() {
        int j = i;
        while (i < s.length() && Character.isDigit(s.charAt(i)))
            i++;
        text = s.substring(j, i);
        type = TokenType.INT;
    }

    private void readString() {
        i++; // skip '
        int j = i;
        while (i < s.length() && s.charAt(i) != '\'')
            i++;
        if (i >= s.length())
            throw error("unterminated string");
        text = s.substring(j, i);
        i++; // skip closing '
        type = TokenType.STRING;
    }

    private void readIdentOrKeyword() {
        int j = i;
        while (i < s.length() && isIdentPart(s.charAt(i)))
            i++;
        String raw = s.substring(j, i);
        String u = raw.toUpperCase(Locale.ROOT);
        switch (u) {
            case "SELECT":
                type = TokenType.SELECT;
                break;
            case "DISTINCT":
                type = TokenType.DISTINCT;
                break;
            case "FROM":
                type = TokenType.FROM;
                break;
            case "WHERE":
                type = TokenType.WHERE;
                break;
            case "JOIN":
                type = TokenType.JOIN;
                break;
            case "ON":
                type = TokenType.ON;
                break;
            case "AND":
                type = TokenType.AND;
                break;
            case "GROUP":
                type = TokenType.GROUP;
                break;
            case "HAVING":
                type = TokenType.HAVING;
                break;
            case "COUNT":
                type = TokenType.COUNT;
                break;
            case "SUM":
                type = TokenType.SUM;
                break;
            case "AVG":
                type = TokenType.AVG;
                break;
            case "MIN":
                type = TokenType.MIN;
                break;
            case "MAX":
                type = TokenType.MAX;
                break;
            case "ORDER":
                type = TokenType.ORDER;
                break;
            case "BY":
                type = TokenType.BY;
                break;
            case "LIMIT":
                type = TokenType.LIMIT;
                break;
            case "ASC":
                type = TokenType.ASC;
                break;
            case "DESC":
                type = TokenType.DESC;
                break;
            case "CREATE":
                type = TokenType.CREATE;
                break;
            case "INDEX":
                type = TokenType.INDEX;
                break;
            case "USING":
                type = TokenType.USING;
                break;
            case "BTREE":
                type = TokenType.BTREE;
                break;
            default:
                type = TokenType.IDENT;
                // raw = raw;
                break;
        }
        text = raw;
    }

    private void skipWs() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i)))
            i++;
    }

    private boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private RuntimeException error(String m) {
        return new RuntimeException("Lexer: " + m);
    }
}
