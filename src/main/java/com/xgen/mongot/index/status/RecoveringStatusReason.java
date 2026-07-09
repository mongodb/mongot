package com.xgen.mongot.index.status;

public enum RecoveringStatusReason implements StatusReason {
  VIEW_PIPELINE_ERROR(
      "View pipeline error: Replication encountered an error. Once the problem is fixed, indexing"
          + " will resume automatically. Please examine details and make changes to documents or"
          + " the view pipeline, if needed:%s"),
  RECOVERING(
      "Recovering: The index has encountered an issue and " + "is attempting automatic recovery."),
  QUERY_REJECTED_BY_SETTINGS(
      "Replication rejected by query settings: A replication command was blocked by an operation"
          + " rejection filter (setQuerySettings with reject: true). The index will keep retrying"
          + " until the rejection filter is removed. Remove the operation rejection filter for the"
          + " affected query shape (removeQuerySettings) to allow replication to resume."
          + " Details: %s");
  private final String text;

  RecoveringStatusReason(String text) {
    this.text = text;
  }

  @Override
  public String formatMessage(Object... details) {
    return StatusReason.getString(this.text, details);
  }
}
