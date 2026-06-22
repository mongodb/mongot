package com.xgen.mongot.index.analyzer.attributes;

/**
 * A token stream is a chain of tokens, each comprised of a number of characters. We can think of a
 * token stream as a directed acyclic graph - each node is a position in the token stream, and each
 * arc is a token.
 *
 * <p>See this blog post:
 * https://blog.mikemccandless.com/2012/04/lucenes-tokenstreams-are-actually.html
 */
public enum TokenStreamType {
  /**
   * Token streams that have exactly one arc between each position node are {@code STREAM}s. Normal
   * written sentences are "stream"-type.
   */
  STREAM,

  /**
   * Token streams that might have more than one arc between each position node are {@code GRAPH}s.
   * We can create a "graph" token stream by adding new "synonym" tokens to a normal written
   * sentence.
   */
  GRAPH;

  public boolean isGraph() {
    return this == GRAPH;
  }
}
