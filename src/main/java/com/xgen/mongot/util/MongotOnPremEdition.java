package com.xgen.mongot.util;

/** Edition of the on-premises mongot server, as opposed to the Atlas-managed deployment. */
public enum MongotOnPremEdition {
  COMMUNITY("mongot-community"),
  ENTERPRISE("mongot-enterprise");

  private final String mongotHostName;

  MongotOnPremEdition(String mongotHostName) {
    this.mongotHostName = mongotHostName;
  }

  /**
   * The name mongot reports about itself in explain metadata and outbound HTTP user agents, e.g.
   * {@code "mongot-community"}.
   */
  public String mongotHostName() {
    return this.mongotHostName;
  }
}
