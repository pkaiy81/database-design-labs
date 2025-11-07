package app.sql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParserDmlTest {

    @Test
    void parseInsertStatementProducesAst() {
        Ast.Statement stmt = new Parser("INSERT INTO people(id, name) VALUES (1, 'Ada')").parseStatement();
        Ast.InsertStmt insert = assertInstanceOf(Ast.InsertStmt.class, stmt);

        assertEquals("people", insert.table);
        assertEquals(List.of("id", "name"), insert.columns);
        assertEquals(1, ((Ast.Expr.I) insert.values.get(0)).v);
        assertEquals("Ada", ((Ast.Expr.S) insert.values.get(1)).v);
    }

    @Test
    void parseUpdateStatementProducesAssignmentsAndWhereClause() {
        String sql = "UPDATE people SET id = 2, name = 'Alan' WHERE id = 1";
        Ast.Statement stmt = new Parser(sql).parseStatement();
        Ast.UpdateStmt update = assertInstanceOf(Ast.UpdateStmt.class, stmt);

        assertEquals("people", update.table);
        assertEquals(2, update.assignments.size());
        Ast.UpdateStmt.Assignment idAssignment = update.assignments.get(0);
        assertEquals("id", idAssignment.column);
        assertEquals(2, ((Ast.Expr.I) idAssignment.value).v);
        Ast.UpdateStmt.Assignment nameAssignment = update.assignments.get(1);
        assertEquals("name", nameAssignment.column);
        assertEquals("Alan", ((Ast.Expr.S) nameAssignment.value).v);

        assertEquals(1, update.where.size());
        Ast.PredicateCompare where = assertInstanceOf(Ast.PredicateCompare.class, update.where.get(0));
        Ast.Expr.Col left = assertInstanceOf(Ast.Expr.Col.class, where.left);
        assertEquals("id", left.name);
        Ast.Expr.I right = assertInstanceOf(Ast.Expr.I.class, where.right);
        assertEquals(1, right.v);
        assertEquals("=", where.op);
    }

    @Test
    void parseDeleteStatementProducesWhereClause() {
        Ast.Statement stmt = new Parser("DELETE FROM people WHERE id = 3").parseStatement();
        Ast.DeleteStmt delete = assertInstanceOf(Ast.DeleteStmt.class, stmt);

        assertEquals("people", delete.table);
        assertEquals(1, delete.where.size());
        Ast.PredicateCompare predicate = assertInstanceOf(Ast.PredicateCompare.class, delete.where.get(0));
        Ast.Expr.Col left = assertInstanceOf(Ast.Expr.Col.class, predicate.left);
        assertEquals("id", left.name);
        Ast.Expr.I right = assertInstanceOf(Ast.Expr.I.class, predicate.right);
        assertEquals(3, right.v);
    }

    @Test
    void insertWithMismatchedColumnValueCountThrows() {
        Parser parser = new Parser("INSERT INTO people(id, name) VALUES (1)");
        RuntimeException error = assertThrows(RuntimeException.class, parser::parseStatement);
        assertTrue(error.getMessage().contains("columns and values count mismatch"));
    }
}
