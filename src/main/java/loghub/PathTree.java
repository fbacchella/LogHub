package loghub;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import lombok.Getter;

public class PathTree<T, V> {

    // data structure that represents a node in the tree
    private static class Node<T, V> {
        @Getter
        private V value;
        private final Map<T, Node<T, V>> children = new ConcurrentHashMap<>();
        public Node(V value) {
            this.value = value;
        }
        @Override
        public String toString() {
            return String.format("%s(%s)", value, children);
        }
    }

    private final Node<T, V> root;

    public PathTree(V rootValue) {
        this.root = new Node<>(rootValue);
    }

    public V findByPath(T[] path) {
        Node<T, V> current = root;
        for (T t : path) {
            if (current.children.isEmpty()) {
                return null;
            } else {
                current = current.children.get(t);
            }
        }
        return current.value;
    }

    public void add(T[] path, V v) {
        Node<T, V> current = root;
        for (int i = 0; i < path.length - 1; i++) {
            current = current.children.computeIfAbsent(path[i], k -> new Node<>(null));
        }
        Node<T, V> newNode = new Node<>(v);
        current.children.put(path[path.length -1], newNode);
    }

    public V computeIfAbsent(T[] path, Supplier<V> supplier) {
        Node<T, V> current = root;
        for (int i = 0; i < path.length - 1; i++) {
            current = current.children.computeIfAbsent(path[i], k -> new Node<>(null));
        }
        return current.children.compute(path[path.length - 1], (k, v) -> resolveNodeWithValue(v, supplier)).value;
    }

    public V computeChildIfAbsent(T[] path, T child, Supplier<V> supplier) {
        Node<T, V> current = root;
        for (T t : path) {
            current = current.children.computeIfAbsent(t, k -> new Node<>(null));
        }
        return current.children.compute(child, (k, v) -> resolveNodeWithValue(v, supplier)).value;
    }

    private Node<T, V> resolveNodeWithValue(Node<T, V> node, Supplier<V> supplier) {
        if (node == null) {
            return new Node<>(supplier.get());
        } else {
            if (node.value == null) {
                node.value = supplier.get();
            }
            return node;
        }
    }

    @Override
    public String toString() {
        return String.format("Tree(%s)", root);
    }

}
