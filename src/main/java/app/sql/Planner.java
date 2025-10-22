package app.sql;

import app.index.IndexRegistry;
import app.index.SearchKey;
import app.index.btree.BTreeIndex;
import app.index.btree.BTreeRangeScan;
import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class Planner {
    private final FileMgr fm;
    private final MetadataManager mdm;
    private final IndexRegistry idxReg;

    public Planner(FileMgr fm, MetadataManager mdm) {
        this(fm, mdm, null);
    }

    public Planner(FileMgr fm, MetadataManager mdm, IndexRegistry idxReg) {
        this.fm = fm;
        this.mdm = mdm;
        this.idxReg = idxReg;
    }

    public Scan plan(String sql) {
        Ast.Statement stmt = new Parser(sql).parseStatement();
        if (stmt instanceof Ast.SelectStmt select)
            return plan(select);
        throw new IllegalArgumentException("planner.plan(String) supports SELECT statements only");
    }

    public PreparedPlan prepare(Ast.SelectStmt ast) {

        Layout baseLayout = mdm.getLayout(ast.from.table);
        TableFile baseTf = new TableFile(fm, ast.from.table + ".tbl", baseLayout);
        LinkedHashSet<String> availableFields = new LinkedHashSet<>(baseLayout.schema().fields().keySet());

        boolean skipWhereProcessing = false;
        boolean orderHandled = false;
        boolean limitHandled = false;

        IndexOrderPlan indexOrderPlan = tryPlanIndexOrder(ast, baseLayout, baseTf);

        Scan s;
        PlanNode planNode;
        boolean usedIndexForWhere = indexOrderPlan != null;
        if (indexOrderPlan != null) {
            s = indexOrderPlan.scan;
            planNode = indexOrderPlan.planNode;
            skipWhereProcessing = true;
            orderHandled = true;
            limitHandled = indexOrderPlan.limitHandled;
        } else {
            s = new TableScan(fm, baseTf);
            planNode = node("TableScan", mapOf("table", ast.from.table));
        }

        for (Ast.Join j : ast.joins) {
            Layout rightLayout = mdm.getLayout(j.table);
            TableFile rightTf = new TableFile(fm, j.table + ".tbl", rightLayout);
            availableFields.addAll(rightLayout.schema().fields().keySet());

            String leftCol = null, rightCol = null;
            if (j.on.left instanceof Ast.Expr.Col && j.on.right instanceof Ast.Expr.Col) {
                String l = ((Ast.Expr.Col) j.on.left).name;
                String r = ((Ast.Expr.Col) j.on.right).name;
                leftCol = stripQualifier(l);
                rightCol = stripQualifier(r);
            }

            PlanNode rightPlanNode = node("TableScan", mapOf("table", j.table));
            boolean usedIndex = false;
            if (idxReg != null && rightCol != null) {
                Optional<app.index.HashIndex> opt = idxReg.findHashIndex(j.table, rightCol);
                if (opt.isPresent()) {
                    System.out.println("[PLAN] join using index on " + j.table + "." + rightCol);
                    s = new app.query.IndexJoinScan(
                            s, fm, rightLayout, j.table + ".tbl", opt.get(), leftCol, rightCol);
                    planNode = node("IndexJoin", mapOf(
                            "table", j.table,
                            "indexCol", rightCol,
                            "leftCol", leftCol),
                            planNode, rightPlanNode);
                    usedIndex = true;
                }
            }
            if (!usedIndex) {
                System.out.println("[PLAN] join via product + filter (no index)");
                Scan rightScan = new TableScan(fm, rightTf);
                s = new ProductScan(s, rightScan);
                planNode = node("NestedLoopJoin", mapOf("table", j.table), planNode, rightPlanNode);
                Predicate predicate = toPredicate(j.on);
                s = new SelectScan(s, predicate);
                planNode = node("Filter", mapOf("pred", predicateToString(j.on)), planNode);
            }
        }

        Ast.Predicate predicateHandledByIndex = null;
        if (!skipWhereProcessing) {
            if (ast.joins.isEmpty()) {
                IndexPlanResult indexPlan = planSingleTableWithPossibleIndex(ast.from.table, ast.where);
                if (indexPlan != null) {
                    s = indexPlan.scan;
                    planNode = indexPlan.planNode;
                    predicateHandledByIndex = indexPlan.predicate;
                    usedIndexForWhere = true;
                }
                if (ast.where != null) {
                    for (Ast.Predicate p : ast.where) {
                        if (p == predicateHandledByIndex)
                            continue;
                        s = new SelectScan(s, toPredicate(p));
                        planNode = node("Filter", mapOf("pred", predicateToString(p)), planNode);
                    }
                }
            } else if (ast.where != null) {
                for (Ast.Predicate p : ast.where) {
                    s = new SelectScan(s, toPredicate(p));
                    planNode = node("Filter", mapOf("pred", predicateToString(p)), planNode);
                }
            }
            if (!usedIndexForWhere && ast.where != null && !ast.where.isEmpty())
                System.out.println("[PLAN] where via scan filter");
        }

        List<String> outputCols = new ArrayList<>();
        boolean hasAgg = ast.projections.stream().anyMatch(it -> it instanceof Ast.SelectItem.Agg);
        if (hasAgg || ast.groupBy != null) {
            String groupField = ast.groupBy != null ? stripQualifier(ast.groupBy) : null;

            List<GroupByScan.Spec> specs = new ArrayList<>();
            for (Ast.SelectItem it : ast.projections) {
                if (it instanceof Ast.SelectItem.Agg a) {
                    app.query.Agg agg = app.query.Agg.valueOf(a.func);
                    String arg = a.arg == null ? null : stripQualifier(a.arg);
                    specs.add(new GroupByScan.Spec(agg, arg));
                }
            }

            s = new GroupByScan(s, groupField, specs);
            planNode = node("GroupBy", mapOf("group", groupField != null ? groupField : "(global)"), planNode);
            System.out.println("[PLAN] group-by/agg applied"
                    + (groupField != null ? " (group: " + groupField + ")" : " (global)"));

            if (groupField != null)
                outputCols.add(groupField);
            for (var sp : specs)
                outputCols.add(sp.outName());
            if (!outputCols.isEmpty()) {
                s = new ProjectScan(s, outputCols);
                planNode = node("Project", mapOf("cols", String.join(",", outputCols)), planNode);
            }

            if (ast.having != null) {
                String outName = toAggOutName(ast.having.func, ast.having.arg);
                var op = switch (ast.having.op) {
                    case ">" -> app.query.HavingScan.Op.GT;
                    case ">=" -> app.query.HavingScan.Op.GE;
                    case "<" -> app.query.HavingScan.Op.LT;
                    case "<=" -> app.query.HavingScan.Op.LE;
                    case "=" -> app.query.HavingScan.Op.EQ;
                    default -> throw new IllegalArgumentException("unsupported HAVING op: " + ast.having.op);
                };
                s = new app.query.HavingScan(s, outName, op, ast.having.rhs);
                planNode = node("Having", mapOf("pred", describeHaving(ast.having)), planNode);
            }
        } else {
            boolean hasStar = ast.projections.stream()
                    .anyMatch(it -> it instanceof Ast.SelectItem.Column
                            && "*".equals(((Ast.SelectItem.Column) it).name));
            if (!hasStar && !ast.projections.isEmpty()) {
                for (Ast.SelectItem it : ast.projections) {
                    if (it instanceof Ast.SelectItem.Column c) {
                        outputCols.add(stripQualifier(c.name));
                    }
                }
                if (!outputCols.isEmpty()) {
                    s = new ProjectScan(s, outputCols);
                    planNode = node("Project", mapOf("cols", String.join(",", outputCols)), planNode);
                }
            } else {
                outputCols.clear();
            }
        }

        if (ast.distinct) {
            List<String> cols;
            if (outputCols.isEmpty()) {
                if (ast.groupBy != null || hasAgg)
                    cols = new java.util.ArrayList<>(outputCols);
                else
                    throw new IllegalArgumentException("DISTINCT with SELECT * is not supported in this minimal impl");
            } else
                cols = outputCols;
            s = new app.query.DistinctScan(s, cols);
            planNode = node("Distinct", mapOf("cols", String.join(",", cols)), planNode);
        }

        if (ast.orderBy != null && !orderHandled) {
            String fld = stripQualifier(ast.orderBy.field);
            List<String> carry;
            if (outputCols.isEmpty()) {
                carry = new ArrayList<>(availableFields);
            } else {
                carry = new ArrayList<>(outputCols);
            }
            if (carry.isEmpty())
                carry.add(fld);
            s = new OrderByScan(s, fld, ast.orderBy.asc, carry);
            planNode = node("Sort", mapOf("keys", fld, "order", ast.orderBy.asc ? "ASC" : "DESC"), planNode);
        }

        if (ast.limit != null && !limitHandled) {
            s = new LimitScan(s, ast.limit);
            planNode = node("Limit", mapOf("k", Integer.toString(ast.limit)), planNode);
        }

        return new PreparedPlan(s, planNode);
    }

    public Scan plan(Ast.SelectStmt ast) {
        return prepare(ast).scan();
    }

    public String explain(Ast.SelectStmt ast) {
        return PlanPrinter.print(prepare(ast).plan());
    }

    public String explain(Ast.ExplainStmt stmt) {
        return explain(stmt.select);
    }

    public int executeInsert(Ast.InsertStmt stmt) {
        String table = stmt.table;
        Layout layout = mdm.getLayout(table);
        Schema schema = layout.schema();
        TableFile tf = new TableFile(fm, table + ".tbl", layout);

        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, table);
            ts.beforeFirst();
            ts.insert();
            for (int i = 0; i < stmt.columns.size(); i++) {
                String col = stripQualifier(stmt.columns.get(i));
                if (!schema.hasField(col))
                    throw new IllegalArgumentException("Unknown column '" + col + "' on table " + table);
                FieldType type = schema.fieldType(col);
                Ast.Expr value = stmt.values.get(i);
                switch (type) {
                    case INT -> ts.setInt(col, expectIntLiteral(value, table, col));
                    case STRING -> ts.setString(col, expectStringLiteral(value, table, col));
                    default -> throw new IllegalArgumentException("Unsupported field type: " + type);
                }
            }
            return 1;
        }
    }

    public int executeUpdate(Ast.UpdateStmt stmt) {
        String table = stmt.table;
        Layout layout = mdm.getLayout(table);
        Schema schema = layout.schema();
        TableFile tf = new TableFile(fm, table + ".tbl", layout);
        List<Predicate> predicates = compilePredicates(stmt.where);

        int updated = 0;
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, table);
            ts.beforeFirst();
            while (ts.next()) {
                if (!matchesWhere(ts, predicates))
                    continue;
                for (Ast.UpdateStmt.Assignment assignment : stmt.assignments) {
                    String col = stripQualifier(assignment.column);
                    if (!schema.hasField(col))
                        throw new IllegalArgumentException("Unknown column '" + col + "' on table " + table);
                    FieldType type = schema.fieldType(col);
                    Ast.Expr value = assignment.value;
                    switch (type) {
                        case INT -> ts.setInt(col, expectIntLiteral(value, table, col));
                        case STRING -> ts.setString(col, expectStringLiteral(value, table, col));
                        default -> throw new IllegalArgumentException("Unsupported field type: " + type);
                    }
                }
                updated++;
            }
        }
        return updated;
    }

    public int executeDelete(Ast.DeleteStmt stmt) {
        String table = stmt.table;
        Layout layout = mdm.getLayout(table);
        TableFile tf = new TableFile(fm, table + ".tbl", layout);
        List<Predicate> predicates = compilePredicates(stmt.where);

        int deleted = 0;
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, table);
            ts.beforeFirst();
            while (ts.next()) {
                if (!matchesWhere(ts, predicates))
                    continue;
                ts.delete();
                deleted++;
            }
        }
        return deleted;
    }

    public boolean executeDropIndex(Ast.DropIndexStmt stmt) {
        return mdm.dropIndex(stmt.indexName);
    }

    public void executeCreateTable(Ast.CreateTableStmt stmt) {
        Schema schema = new Schema();
        for (Ast.CreateTableStmt.ColumnDef col : stmt.columns) {
            switch (col.type) {
                case INT -> schema.addInt(col.name);
                case STRING -> schema.addString(col.name, col.length);
                default -> throw new IllegalArgumentException("unsupported column type: " + col.type);
            }
        }
        mdm.createTable(stmt.tableName, schema);
        Layout layout = mdm.getLayout(stmt.tableName);
        TableFile tf = new TableFile(fm, stmt.tableName + ".tbl", layout);
        if (tf.size() == 0)
            tf.appendFormatted();
    }

    public boolean executeDropTable(Ast.DropTableStmt stmt) {
        return mdm.dropTable(stmt.tableName);
    }

    private int expectIntLiteral(Ast.Expr expr, String table, String column) {
        if (expr instanceof Ast.Expr.I i)
            return i.v;
        throw new IllegalArgumentException("Column '" + column + "' on table " + table + " expects INT literal");
    }

    private String expectStringLiteral(Ast.Expr expr, String table, String column) {
        if (expr instanceof Ast.Expr.S s)
            return s.v;
        throw new IllegalArgumentException(
                "Column '" + column + "' on table " + table + " expects STRING literal");
    }

    private List<Predicate> compilePredicates(List<Ast.Predicate> predicates) {
        if (predicates == null || predicates.isEmpty())
            return List.of();
        List<Predicate> list = new ArrayList<>(predicates.size());
        for (Ast.Predicate p : predicates) {
            try {
                list.add(toPredicate(p));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported predicate in WHERE clause for DML", e);
            }
        }
        return list;
    }

    private boolean matchesWhere(Scan scan, List<Predicate> predicates) {
        for (Predicate predicate : predicates) {
            if (!predicate.evaluate(scan))
                return false;
        }
        return true;
    }

    private IndexOrderPlan tryPlanIndexOrder(Ast.SelectStmt ast, Layout baseLayout, TableFile baseTf) {
        if (ast.orderBy == null || !ast.orderBy.asc)
            return null;
        if (!ast.joins.isEmpty())
            return null;
        if (ast.distinct || ast.groupBy != null || ast.having != null)
            return null;
        boolean hasAgg = ast.projections.stream().anyMatch(it -> it instanceof Ast.SelectItem.Agg);
        if (hasAgg)
            return null;

        String orderField = stripQualifier(ast.orderBy.field);
        if (orderField == null)
            return null;
        if (!baseLayout.schema().hasField(orderField))
            return null;
        if (baseLayout.schema().fieldType(orderField) != FieldType.INT)
            return null;

        Optional<String> indexNameOpt = mdm.findIndexOn(ast.from.table, orderField);
        if (indexNameOpt.isEmpty())
            return null;

        Integer lowValue = null;
        boolean lowInclusive = true;
        Integer highValue = null;
        boolean highInclusive = true;
    Integer eqValue = null;

    List<Predicate> residualPredicates = new ArrayList<>();
    List<String> residualDescriptions = new ArrayList<>();

        List<Ast.Predicate> wherePredicates = (ast.where == null) ? List.of() : ast.where;
        for (Ast.Predicate predicate : wherePredicates) {
            String column = extractColumn(predicate);
            boolean onOrderColumn = column != null && stripQualifier(column).equals(orderField);
            if (onOrderColumn) {
                Integer eqVal = extractEqValue(predicate);
                if (eqVal != null) {
                    if (eqValue != null && !eqValue.equals(eqVal))
                        return null;
                    eqValue = eqVal;
                    continue;
                }

                RangeBound range = extractRange(predicate);
                if (range != null) {
                    if (range.loKey != null) {
                        int candidate = range.loKey.asInt();
                        if (lowValue == null || candidate > lowValue) {
                            lowValue = candidate;
                            lowInclusive = range.loInclusive;
                        } else if (candidate == lowValue) {
                            lowInclusive = lowInclusive && range.loInclusive;
                        }
                    }
                    if (range.hiKey != null) {
                        int candidate = range.hiKey.asInt();
                        if (highValue == null || candidate < highValue) {
                            highValue = candidate;
                            highInclusive = range.hiInclusive;
                        } else if (candidate == highValue) {
                            highInclusive = highInclusive && range.hiInclusive;
                        }
                    }
                    continue;
                }

                return null;
            }

            try {
                residualPredicates.add(toPredicate(predicate));
                residualDescriptions.add(predicateToString(predicate));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        if (eqValue != null) {
            if (lowValue != null && (eqValue < lowValue || (eqValue.equals(lowValue) && !lowInclusive)))
                return null;
            if (highValue != null && (eqValue > highValue || (eqValue.equals(highValue) && !highInclusive)))
                return null;
            lowValue = eqValue;
            highValue = eqValue;
            lowInclusive = true;
            highInclusive = true;
        }

        if (lowValue != null && highValue != null) {
            if (lowValue > highValue)
                return null;
            if (lowValue.equals(highValue) && (!lowInclusive || !highInclusive))
                return null;
        }

        SearchKey lowKey = (lowValue != null) ? keyInt(lowValue) : null;
        SearchKey highKey = (highValue != null) ? keyInt(highValue) : null;

        int limit = (ast.limit != null) ? Math.max(0, ast.limit) : Integer.MAX_VALUE;
        IndexOrderScan scan = new IndexOrderScan(
                fm,
                baseTf,
                indexNameOpt.get(),
                lowKey,
                lowInclusive,
                highKey,
                highInclusive,
                residualPredicates,
                limit);

    System.out.println("[PLAN] order-by via BTree index on " + ast.from.table + "." + orderField
        + (ast.limit != null ? " (limit " + ast.limit + ")" : ""));

    Map<String, String> scanProps = mapOf(
        "table", ast.from.table,
        "index", indexNameOpt.get(),
        "order", "ASC");
    if (ast.limit != null)
        scanProps.put("limit", Integer.toString(ast.limit));
    PlanNode rangeNode = makeRangeNode(lowValue, lowInclusive, highValue, highInclusive);
    PlanNode tail = rangeNode;
    if (!residualDescriptions.isEmpty()) {
        PlanNode filterNode = node("Filter", mapOf("pred", String.join(" AND ", residualDescriptions)),
            tail);
        tail = filterNode;
    }
    PlanNode plan = node("IndexOrderScan", scanProps, tail);

    return new IndexOrderPlan(scan, ast.limit != null, plan);
    }

    private static String stripQualifier(String name) {
        return (name != null && name.contains(".")) ? name.substring(name.indexOf('.') + 1) : name;
    }

    private Predicate toPredicate(Ast.Predicate p) {
        if (p instanceof Ast.PredicateCompare compare) {
            if (!(compare.left instanceof Ast.Expr.Col leftCol))
                throw new IllegalArgumentException("unsupported predicate: left-hand side is not a column");
            String left = stripQualifier(leftCol.name);
            Ast.Expr right = compare.right;
            return switch (compare.op) {
                case "=" -> {
                    if (right instanceof Ast.Expr.Col rc)
                        yield Predicate.eqField(left, stripQualifier(rc.name));
                    if (right instanceof Ast.Expr.I ri)
                        yield Predicate.compareInt(left, Predicate.Op.EQ, ri.v);
                    if (right instanceof Ast.Expr.S rs)
                        yield Predicate.eqString(left, rs.v);
                    throw new IllegalArgumentException("unsupported '=' predicate rhs: " + right.getClass());
                }
                case "<" -> Predicate.compareInt(left, Predicate.Op.LT, expectIntLiteral(right, left));
                case "<=" -> Predicate.compareInt(left, Predicate.Op.LE, expectIntLiteral(right, left));
                case ">" -> Predicate.compareInt(left, Predicate.Op.GT, expectIntLiteral(right, left));
                case ">=" -> Predicate.compareInt(left, Predicate.Op.GE, expectIntLiteral(right, left));
                default -> throw new IllegalArgumentException("unsupported comparison operator: " + compare.op);
            };
        }
        if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.Col) {
            return Predicate.eqField(stripQualifier(((Ast.Expr.Col) p.left).name),
                    stripQualifier(((Ast.Expr.Col) p.right).name));
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.I) {
            return Predicate.compareInt(stripQualifier(((Ast.Expr.Col) p.left).name), Predicate.Op.EQ,
                    ((Ast.Expr.I) p.right).v);
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.S) {
            return Predicate.eqString(stripQualifier(((Ast.Expr.Col) p.left).name),
                    ((Ast.Expr.S) p.right).v);
        }
        throw new IllegalArgumentException("unsupported predicate");
    }

    private static int expectIntLiteral(Ast.Expr expr, String field) {
        if (expr instanceof Ast.Expr.I ri)
            return ri.v;
        throw new IllegalArgumentException("Expected integer literal for comparison on field '" + field + "'");
    }

    private static String strip(String name) {
        return (name != null && name.contains(".")) ? name.substring(name.indexOf('.') + 1) : name;
    }

    private static String toAggOutName(String func, String arg) {
        String a = (arg == null) ? null : strip(arg);
        return switch (func) {
            case "COUNT" -> "count";
            case "SUM" -> "sum_" + a;
            case "AVG" -> "avg_" + a;
            case "MIN" -> "min_" + a;
            case "MAX" -> "max_" + a;
            default -> throw new IllegalArgumentException("unknown agg: " + func);
        };
    }

    private IndexPlanResult planSingleTableWithPossibleIndex(String tableName, List<Ast.Predicate> predicates) {
        if (predicates == null || predicates.isEmpty())
            return null;

        for (Ast.Predicate predicate : predicates) {
            String column = extractColumn(predicate);
            if (column == null)
                continue;
            String colName = stripQualifier(column);
            Optional<String> idxNameOpt = mdm.findIndexOn(tableName, colName);
            if (idxNameOpt.isEmpty())
                continue;
            String idxName = idxNameOpt.get();

            Integer eqVal = extractEqValue(predicate);
            if (eqVal != null) {
                System.out.println("[PLAN] where using BTree index (EQ) on " + tableName + "." + colName);
                Scan scan = new BTreeEqScan(fm, mdm, tableName, idxName, eqVal);
                PlanNode plan = node("IndexEqScan", mapOf(
                        "table", tableName,
                        "index", idxName,
                        "key", Integer.toString(eqVal)));
                return new IndexPlanResult(scan, predicate, plan);
            }

            RangeBound range = extractRange(predicate);
            if (range != null) {
                System.out.println("[PLAN] where using BTree index (RANGE) on " + tableName + "." + colName);
                try {
                    BTreeIndex idx = new BTreeIndex(fm, idxName, tableName + ".tbl");
                    idx.open();
                    Layout layout = mdm.getLayout(tableName);
                    TableFile tf = new TableFile(fm, tableName + ".tbl", layout);
                    TableScan ts = new TableScan(fm, tf);
                    ts.beforeFirst();
                    Scan scan = new BTreeRangeScan(ts, idx, range.loKey, range.loInclusive, range.hiKey,
                            range.hiInclusive);
            PlanNode rangeNode = makeRangeNode(
                range.loKey != null ? range.loKey.asInt() : null,
                range.loInclusive,
                range.hiKey != null ? range.hiKey.asInt() : null,
                range.hiInclusive);
            PlanNode plan = node("IndexRangeScan", mapOf(
                "table", tableName,
                "index", idxName),
                rangeNode);
            return new IndexPlanResult(scan, predicate, plan);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to build BTree range plan for "
                            + tableName + "." + colName, e);
                }
            }
        }
        return null;
    }

    private RangeBound extractRange(Ast.Predicate p) {
        if (p instanceof Ast.PredicateBetween between) {
            return new RangeBound(keyInt(between.low), true, keyInt(between.high), true);
        }
        if (p instanceof Ast.PredicateCompare compare && compare.right instanceof Ast.Expr.I val) {
            return switch (compare.op) {
                case ">=" -> new RangeBound(keyInt(val.v), true, null, false);
                case ">" -> new RangeBound(keyInt(val.v), false, null, false);
                case "<=" -> new RangeBound(null, false, keyInt(val.v), true);
                case "<" -> new RangeBound(null, false, keyInt(val.v), false);
                default -> null;
            };
        }
        return null;
    }

    private String extractColumn(Ast.Predicate p) {
        if (p == null)
            return null;
        Ast.Expr left = p.left;
        if (left instanceof Ast.Expr.Col col)
            return stripQualifier(col.name);
        return null;
    }

    private Integer extractEqValue(Ast.Predicate p) {
        if (p instanceof Ast.PredicateCompare compare && "=".equals(compare.op)
                && compare.right instanceof Ast.Expr.I valFromCompare) {
            return valFromCompare.v;
        }
        return null;
    }

    private SearchKey keyInt(int v) {
        return SearchKey.ofInt(v);
    }

    private static Map<String, String> mapOf(Object... keyValues) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        if (keyValues == null)
            return map;
        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("mapOf requires even number of arguments");
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            Object value = keyValues[i + 1];
            if (key == null || value == null)
                continue;
            map.put(key.toString(), value.toString());
        }
        return map;
    }

    private static PlanNode node(String name, Map<String, String> props, PlanNode... children) {
        List<PlanNode> list = new ArrayList<>();
        if (children != null) {
            for (PlanNode child : children) {
                if (child != null)
                    list.add(child);
            }
        }
        return new PlanNode(name, props, list);
    }

    private static PlanNode makeRangeNode(Integer lowValue, boolean lowInclusive, Integer highValue, boolean highInclusive) {
        if (lowValue == null && highValue == null)
            return null;
        Map<String, String> props = mapOf(
                "lo", formatBound(lowValue, "-"),
                "hi", formatBound(highValue, "+"));
        if (lowValue != null && !lowInclusive)
            props.put("loInclusive", "false");
        if (highValue != null && !highInclusive)
            props.put("hiInclusive", "false");
        return node("Range", props);
    }

    private static String formatBound(Integer value, String whenNull) {
        return value == null ? whenNull : value.toString();
    }

    private String predicateToString(Ast.Predicate predicate) {
        if (predicate == null)
            return "";
        if (predicate instanceof Ast.PredicateBetween between) {
            return stripQualifier(((Ast.Expr.Col) between.left).name)
                    + " BETWEEN " + between.low + " AND " + between.high;
        }
        String left = exprToString(predicate.left);
        if (predicate instanceof Ast.PredicateCompare compare) {
            return left + compare.op + exprToString(compare.right);
        }
        if (predicate.right instanceof Ast.Expr.I ri)
            return left + "=" + ri.v;
        if (predicate.right instanceof Ast.Expr.S rs)
            return left + "='" + rs.v + "'";
        if (predicate.right instanceof Ast.Expr.Col rc)
            return left + "=" + exprToString(rc);
        return left;
    }

    private String exprToString(Ast.Expr expr) {
        if (expr == null)
            return "";
        if (expr instanceof Ast.Expr.Col col)
            return stripQualifier(col.name);
        if (expr instanceof Ast.Expr.I i)
            return Integer.toString(i.v);
        if (expr instanceof Ast.Expr.S s)
            return "'" + s.v + "'";
        return "?";
    }

    private String describeHaving(Ast.Having having) {
        String arg = having.arg == null ? "*" : stripQualifier(having.arg);
        return having.func + "(" + arg + ")" + having.op + having.rhs;
    }

    public static final class PreparedPlan {
        private final Scan scan;
        private final PlanNode plan;

        PreparedPlan(Scan scan, PlanNode plan) {
            this.scan = Objects.requireNonNull(scan);
            this.plan = Objects.requireNonNull(plan);
        }

        public Scan scan() {
            return scan;
        }

        public PlanNode plan() {
            return plan;
        }
    }

    private static final class RangeBound {
        final SearchKey loKey;
        final boolean loInclusive;
        final SearchKey hiKey;
        final boolean hiInclusive;

        RangeBound(SearchKey loKey, boolean loInclusive, SearchKey hiKey, boolean hiInclusive) {
            this.loKey = loKey;
            this.loInclusive = loInclusive;
            this.hiKey = hiKey;
            this.hiInclusive = hiInclusive;
        }
    }

    private static final class IndexPlanResult {
        final Scan scan;
        final Ast.Predicate predicate;
        final PlanNode planNode;

        IndexPlanResult(Scan scan, Ast.Predicate predicate, PlanNode planNode) {
            this.scan = scan;
            this.predicate = predicate;
            this.planNode = planNode;
        }
    }

    private static final class IndexOrderPlan {
        final Scan scan;
        final boolean limitHandled;
        final PlanNode planNode;

        IndexOrderPlan(Scan scan, boolean limitHandled, PlanNode planNode) {
            this.scan = scan;
            this.limitHandled = limitHandled;
            this.planNode = planNode;
        }
    }

}