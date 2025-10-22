package app.sql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PlanNode {
    private final String name;
    private final Map<String, String> props;
    private final List<PlanNode> children;

    public PlanNode(String name) {
        this(name, Map.of(), List.of());
    }

    public PlanNode(String name, Map<String, String> props) {
        this(name, props, List.of());
    }

    public PlanNode(String name, Map<String, String> props, List<PlanNode> children) {
        this.name = Objects.requireNonNull(name, "name");
        this.props = Collections.unmodifiableMap(new LinkedHashMap<>(props == null ? Map.of() : props));
        this.children = List.copyOf(children == null ? List.of() : children);
    }

    public String name() {
        return name;
    }

    public Map<String, String> props() {
        return props;
    }

    public List<PlanNode> children() {
        return children;
    }
}
