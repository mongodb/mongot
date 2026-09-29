package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.SingleLinkedList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A {@link BsonParseContext} that collects exceptions that would have been raised due to unknown
 * fields instead of throwing them.
 *
 * <p>Note: unlike {@link BsonParseContext}, each call to {@link #root()} returns a new instance.
 */
public class PermissiveBsonParseContext extends BsonParseContext {

  private final List<BsonParseException> unknownFieldExceptions;

  PermissiveBsonParseContext(
      SingleLinkedList<String> ancestors, List<BsonParseException> unknownFieldExceptions) {
    this(ancestors, unknownFieldExceptions, Optional.empty());
  }

  PermissiveBsonParseContext(
      SingleLinkedList<String> ancestors,
      List<BsonParseException> unknownFieldExceptions,
      Optional<DeploymentEnvironment> deploymentEnvironment) {
    super(ancestors, deploymentEnvironment);
    this.unknownFieldExceptions = unknownFieldExceptions;
  }

  public static PermissiveBsonParseContext root() {
    return root(Optional.empty());
  }

  public static PermissiveBsonParseContext root(
      Optional<DeploymentEnvironment> deploymentEnvironment) {
    return new PermissiveBsonParseContext(
        BsonParseContext.root().getHierarchy(), new ArrayList<>(), deploymentEnvironment);
  }

  @Override
  public PermissiveBsonParseContext child(String segment) {
    return new PermissiveBsonParseContext(
        this.getHierarchy().prepend(segment),
        this.unknownFieldExceptions,
        this.getDeploymentEnvironment());
  }

  @Override
  public PermissiveBsonParseContext arrayElement(int index) {
    // Add [index] to path without adding a delimiter before it.
    if (this.getHierarchy().isEmpty()) {
      return new PermissiveBsonParseContext(
          this.getHierarchy().prepend("[" + index + ']'),
          this.unknownFieldExceptions,
          this.getDeploymentEnvironment());
    } else {
      String elementPath = this.getHierarchy().head() + "[" + index + ']';
      return new PermissiveBsonParseContext(
          this.getHierarchy().getTail().prepend(elementPath),
          this.unknownFieldExceptions,
          this.getDeploymentEnvironment());
    }
  }

  @Override
  public void handleUnexpectedFields(Collection<String> fields, Collection<String> expectedFields) {
    try {
      super.handleUnexpectedFields(fields, expectedFields);
    } catch (BsonParseException e) {
      this.unknownFieldExceptions.add(e);
    }
  }

  public List<BsonParseException> getUnknownFieldExceptions() {
    return this.unknownFieldExceptions;
  }
}
