package app.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public final class PlanPrinter {
    private PlanPrinter() {
    }

    public static String print(PlanNode root) {
        if (root == null)
            return "";
        List<String> lines = new ArrayList<>();
        traverse(root, "", true, true, lines);
        return String.join(System.lineSeparator(), lines);
    }

    private static void traverse(PlanNode node, String prefix, boolean isLast, boolean isRoot, List<String> lines) {
        StringBuilder line = new StringBuilder();
        if (!isRoot) {
            line.append(prefix);
            line.append(isLast ? "└─ " : "├─ ");
        }
        line.append(formatNode(node));
        lines.add(line.toString());

        List<PlanNode> children = node.children();
        for (int i = 0; i < children.size(); i++) {
            boolean childLast = (i == children.size() - 1);
            String childPrefix = prefix;
            if (!isRoot) {
                childPrefix += isLast ? "   " : "│  ";
            }
            traverse(children.get(i), childPrefix, childLast, false, lines);
        }
    }

    private static String formatNode(PlanNode node) {
        Map<String, String> props = node.props();
        if (props.isEmpty())
            return node.name();
        StringJoiner joiner = new StringJoiner(",");
        for (var entry : props.entrySet())
            joiner.add(entry.getKey() + "=" + entry.getValue());
        return node.name() + "(" + joiner + ")";
    }
}
