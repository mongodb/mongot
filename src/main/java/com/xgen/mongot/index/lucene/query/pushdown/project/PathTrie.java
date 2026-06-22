package com.xgen.mongot.index.lucene.query.pushdown.project;

import com.google.common.base.Joiner;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.jetbrains.annotations.Nullable;

/**
 * A TrieMap implementation where a prefix follows semantics of dotted paths. For example: "a" is
 * not a prefix of "ab" but it is a prefix of "a.b"
 */
@NotThreadSafe
class PathTrie<T> {

  private Optional<T> value = Optional.empty();
  private final Map<String, PathTrie<T>> children = new HashMap<>();

  /**
   * Associates a value with a given {@link FieldPath}
   *
   * @param key – the field path, relative to the current node, that the given value should be
   *     associated with
   * @param value - the value to be associated with the specified key
   * @return the previous mapping for key if one exists, else empty.
   */
  public Optional<T> put(FieldPath key, T value) {
    @Var PathTrie<T> ptr = this;
    for (String next : key.getSegments()) {
      ptr = ptr.children.computeIfAbsent(next, k -> new PathTrie<>());
    }

    return ptr.setValue(value);
  }

  /**
   * Returns the value to which the specified key is mapped, or empty if this map contains no
   * mapping for the key.
   *
   * <p>More formally, if this map contains a mapping from a key k to a value v such that
   * key.equals(k), then this method returns v; otherwise it returns empty(). (There can be at most
   * one such mapping.)
   */
  public Optional<T> get(FieldPath key) {
    return Optional.ofNullable(getNode(key)).flatMap(x -> x.value);
  }

  /**
   * Returns true if there is a defined mapping for this single segment path or if this segment is
   * an ancestor for a path with a defined mapping.
   *
   * <p>See also: {@link #isValidPrefix(FieldPath)}
   */
  public boolean isValidPrefix(String segment) {
    return this.children.containsKey(segment);
  }

  /**
   * Returns true if there is a defined mapping for this key, or if this key is an ancestor of a
   * path with a defined mapping.
   */
  public boolean isValidPrefix(FieldPath key) {
    return getNode(key) != null;
  }

  @Nullable
  private PathTrie<T> getNode(FieldPath key) {
    @Var
    @Nullable
    PathTrie<T> ptr = this;

    for (String next : key.getSegments()) {
      ptr = ptr.children.get(next);
      if (ptr == null) {
        return null;
      }
    }

    return ptr;
  }

  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator("=");
    return "PathTrie(value="
        + this.value.orElse(null)
        + ", children="
        + mapJoiner.join(this.children)
        + ")";
  }

  public void putAll(Map<? extends FieldPath, ? extends T> map) {
    map.forEach(this::put);
  }

  public PathTrie<T> getChild(String segment) {
    var result = this.children.get(segment);
    return result != null ? result : new PathTrie<>();
  }

  /** Returns the mapping for the current node, or empty if this is not a terminal node. */
  public Optional<T> getValue() {
    return this.value;
  }

  /** The number of distinct immediate children of this node. */
  public int getNumChildren() {
    return this.children.size();
  }

  private Optional<T> setValue(T t) {
    var prev = this.value;
    this.value = Optional.ofNullable(t);
    return prev;
  }
}
