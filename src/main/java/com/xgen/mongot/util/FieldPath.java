package com.xgen.mongot.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.Var;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A note on performance: this class is used in most performance-critical parts of the code (e.g.
 * BsonDocumentConverter). If any changes are needed, consider checking related benchmark results
 * before and after merging new code.
 */
@Immutable
public class FieldPath {

  public static final String DELIMITER = ".";

  private final String path;
  private final String leaf;

  /**
   * Represents a hierarchy of the paths, starting from the current one and followed by its
   * ancestors. For example, path "a.b.c" has the following hierarchy: ["a.b.c", "a.b", "a"]
   */
  private final ImmutableList<FieldPath> hierarchy;

  private FieldPath(String path, String leaf, ImmutableList<FieldPath> ancestors) {
    this.path = path;
    this.leaf = leaf;
    this.hierarchy = ImmutableList.<FieldPath>builder().add(this).addAll(ancestors).build();
  }

  public static FieldPath newRoot(String root) {
    return new FieldPath(root, root, ImmutableList.of());
  }

  public FieldPath newChild(String child) {
    return new FieldPath(this.path + DELIMITER + child, child, this.hierarchy);
  }

  /**
   * Create a new {@link FieldPath} that inserts a new leaf as the highest-level ancestor of this
   * path.
   *
   * <p>For example, calling {@code withNewRoot("z")} on a path "a.b.c". The result of such a call
   * returns a path "z.a.b.c".
   *
   * <p>Path "a.b.c" has hierarchy ["a.b.c", "a.b", "a"] before it is added as a child of path "z".
   * The resulting {@link FieldPath} at "z.a.b.c" after such a call has hierarchy ["z.a.b.c",
   * "z.a.b", "z.a", "z"].
   */
  public FieldPath withNewRoot(String newRoot) {
    Optional<FieldPath> parent = this.getParent();

    // If parent is empty, this node was the root of the previous field path. Create a new root, and
    // insert field path as a child of that new root.
    if (parent.isEmpty()) {
      return parse(newRoot).newChild(this.leaf);
    }

    // This node is not the root of this field path. Add the parent (P) of this node as a child of
    // the new root (R) to get a new parent (P') for this node, then add this node as a child of the
    // modified parent (P').
    return parent.get().withNewRoot(newRoot).newChild(this.leaf);
  }

  public static FieldPath parse(String dottedPath) {
    return fromParts(splitStringIntoParts(dottedPath));
  }

  /**
   * Constructs a FieldPath from a list of parts. Note that each part is expected to not contain
   * dots, so not parsed to find the sub-parts.
   */
  private static FieldPath fromParts(ImmutableList<String> parts) {

    @Var var result = FieldPath.newRoot(parts.get(0));

    for (int i = 1; i < parts.size(); i++) {
      result = result.newChild(parts.get(i));
    }

    return result;
  }

  /**
   * Constructs a FieldPath from the provided parts. Note that each part is expected to not contain
   * dots, so not parsed to find the sub-parts.
   */
  public static FieldPath fromParts(String... parts) {
    return fromParts(ImmutableList.copyOf(parts));
  }

  /**
   * Returns a hierarchy of the paths, starting from the current one and followed by its ancestors.
   */
  public ImmutableList<FieldPath> getPathHierarchy() {
    return this.hierarchy.reverse();
  }

  public boolean isAtRoot() {
    return this.hierarchy.size() == 1;
  }

  public boolean isNested() {
    return !isAtRoot();
  }

  /** Returns the final component of the path. For example, for `a.b.c` the leaf would be `c`. */
  public String getLeaf() {
    return this.leaf;
  }

  /** Returns the parent path, or an empty optional if this path is at the root. */
  public Optional<FieldPath> getParent() {
    return isAtRoot() ? Optional.empty() : Optional.of(this.hierarchy.get(1));
  }

  /**
   * Returns a stream of all the sub-paths of this path starting from its parent. For example,
   * ancestorPaths for `a.b.c.d` would return `a.b.c`, `a.b`, and `a`.
   */
  public Stream<FieldPath> ancestorPaths() {
    return isAtRoot() ? Stream.empty() : this.hierarchy.get(1).hierarchy.stream();
  }

  /** Returns true if `this` is a strict descendant of `maybeParent` */
  public boolean isChildOf(FieldPath maybeParent) {
    if (this.hierarchy.size() > maybeParent.hierarchy.size()) {
      return isDirectRelation(maybeParent);
    }
    return false;
  }

  /** Returns true if `other` is either an ancestor, descendant, or equal to `this` FieldPath. */
  public boolean isDirectRelation(FieldPath other) {
    int numSegments = this.hierarchy.size();
    int otherSegments = other.hierarchy.size();
    if (numSegments >= otherSegments) {
      // note hierarchy[0] is `this`, hierarchy[1] is parent, etc
      return this.hierarchy.get(numSegments - otherSegments).equals(other);
    } else {
      return other.hierarchy.get(otherSegments - numSegments).equals(this);
    }
  }

  @Override
  public String toString() {
    return this.path;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof FieldPath)) {
      return false;
    }

    return this.path.equals(((FieldPath) other).path);
  }

  @Override
  public int hashCode() {
    return this.path.hashCode();
  }

  /**
   * Returns a list of parts of the path, split by the delimiter. We pass -1 as the limit to avoid
   * removing trailing empty strings, since a path like `a.b..` is a valid use case.
   */
  private static ImmutableList<String> splitStringIntoParts(String dottedPath) {
    // https://stackoverflow.com/a/16578721
    return ImmutableList.copyOf(dottedPath.split(Pattern.quote(DELIMITER), -1));
  }

  /**
   * Return an unmodifiable view over the segments that compose this path, starting from the
   * top-most ancestor. e.g. "foo.bar" yields ["foo", "bar"]
   */
  public Iterable<String> getSegments() {
    return Lists.transform(getPathHierarchy(), FieldPath::getLeaf);
  }
}
