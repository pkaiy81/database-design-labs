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
import java.util.List;
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

    public Scan plan(Ast.SelectStmt ast) {

        // === FROM ===
        Layout baseLayout = mdm.getLayout(ast.from.table);
        TableFile baseTf = new TableFile(fm, ast.from.table + ".tbl", baseLayout);

        boolean skipWhereProcessing = false;
        boolean orderHandled = false;
        boolean limitHandled = false;

        IndexOrderPlan indexOrderPlan = tryPlanIndexOrder(ast, baseLayout, baseTf);

        Scan s;
        boolean usedIndexForWhere = indexOrderPlan != null;
        if (indexOrderPlan != null) {
            s = indexOrderPlan.scan;
            skipWhereProcessing = true;
            orderHandled = true;
            limitHandled = indexOrderPlan.limitHandled;
        } else {
            s = new TableScan(fm, baseTf);
        }

        // === JOIN（既存の HashIndex 経路は温存）===
        for (Ast.Join j : ast.joins) {
            Layout rightLayout = mdm.getLayout(j.table);
            TableFile rightTf = new TableFile(fm, j.table + ".tbl", rightLayout);

            String leftCol = null, rightCol = null;
            if (j.on.left instanceof Ast.Expr.Col && j.on.right instanceof Ast.Expr.Col) {
                String l = ((Ast.Expr.Col) j.on.left).name;
                String r = ((Ast.Expr.Col) j.on.right).name;
                leftCol = stripQualifier(l);
                rightCol = stripQualifier(r);
            }

            boolean usedIndex = false;
            if (idxReg != null && rightCol != null) {
                Optional<app.index.HashIndex> opt = idxReg.findHashIndex(j.table, rightCol);
                if (opt.isPresent()) {
                    System.out.println("[PLAN] join using index on " + j.table + "." + rightCol);
                    s = new app.query.IndexJoinScan(
                            s, fm, rightLayout, j.table + ".tbl", opt.get(), leftCol, rightCol);
                    usedIndex = true;
                }
            }
            if (!usedIndex) {
                System.out.println("[PLAN] join via product + filter (no index)");
                s = new ProductScan(s, new TableScan(fm, rightTf));
                s = new SelectScan(s, toPredicate(j.on));
            }
        }

        // === WHERE ===
        // 単一テーブルの場合は B-Tree インデックス最適化を優先的に試す
        Ast.Predicate predicateHandledByIndex = null;
        if (!skipWhereProcessing) {
            if (ast.joins.isEmpty()) {
                IndexPlanResult indexPlan = planSingleTableWithPossibleIndex(ast.from.table, ast.where);
                if (indexPlan != null) {
                    s = indexPlan.scan;
                    predicateHandledByIndex = indexPlan.predicate;
                    usedIndexForWhere = true;
                }
                for (Ast.Predicate p : ast.where) {
                    if (p == predicateHandledByIndex)
                        continue;
                    // （BTree なし or 対応外）→ 従来どおりフィルタ
                    s = new SelectScan(s, toPredicate(p));
                }
            } else {
                for (Ast.Predicate p : ast.where) {
                    s = new SelectScan(s, toPredicate(p));
                }
            }
            if (!usedIndexForWhere && ast.where != null && !ast.where.isEmpty())
                System.out.println("[PLAN] where via scan filter");
        }

        // === GROUP BY / AGGREGATION ===（既存そのまま）
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
            System.out.println("[PLAN] group-by/agg applied"
                    + (groupField != null ? " (group: " + groupField + ")" : " (global)"));

            if (groupField != null)
                outputCols.add(groupField);
            for (var sp : specs)
                outputCols.add(sp.outName());
            if (!outputCols.isEmpty())
                s = new ProjectScan(s, outputCols);

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
                if (!outputCols.isEmpty())
                    s = new ProjectScan(s, outputCols);
            } else {
                outputCols.clear();
            }
        }

        // DISTINCT / ORDER BY / LIMIT（既存そのまま）
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
        }

        if (ast.orderBy != null && !orderHandled) {
            String fld = stripQualifier(ast.orderBy.field);
            List<String> carry = outputCols.isEmpty() ? List.of(fld) : outputCols;
            s = new OrderByScan(s, fld, ast.orderBy.asc, carry);
        }

        if (ast.limit != null && !limitHandled)
            s = new LimitScan(s, ast.limit);
        return s;
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

        return new IndexOrderPlan(scan, ast.limit != null);
    }

    private static String stripQualifier(String name) {
        return (name != null && name.contains(".")) ? name.substring(name.indexOf('.') + 1) : name;
    }

    private Predicate toPredicate(Ast.Predicate p) {
        if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.Col) {
            return Predicate.eqField(stripQualifier(((Ast.Expr.Col) p.left).name),
                    stripQualifier(((Ast.Expr.Col) p.right).name));
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.I) {
            return Predicate.eqInt(stripQualifier(((Ast.Expr.Col) p.left).name),
                    ((Ast.Expr.I) p.right).v);
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.S) {
            return Predicate.eqString(stripQualifier(((Ast.Expr.Col) p.left).name),
                    ((Ast.Expr.S) p.right).v);
        }
        throw new IllegalArgumentException("unsupported predicate");
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
                return new IndexPlanResult(scan, predicate);
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
                    return new IndexPlanResult(scan, predicate);
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

        IndexPlanResult(Scan scan, Ast.Predicate predicate) {
            this.scan = scan;
            this.predicate = predicate;
        }
    }

    private static final class IndexOrderPlan {
        final Scan scan;
        final boolean limitHandled;

        IndexOrderPlan(Scan scan, boolean limitHandled) {
            this.scan = scan;
            this.limitHandled = limitHandled;
        }
    }

}