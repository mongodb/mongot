package com.xgen.mongot.index.status;

public enum StaleStatusReason implements StatusReason {
  DOCS_EXCEEDED("Docs exceeded: Index exceeded max docs limit"),
  UNEXPECTED_ERROR("Replication failed: %s");

  private final String text;

  StaleStatusReason(String text) {
    this.text = text;
  }

  @Override
  public String formatMessage(Object... details) {
    return StatusReason.getString(this.text, details);
  }
}
