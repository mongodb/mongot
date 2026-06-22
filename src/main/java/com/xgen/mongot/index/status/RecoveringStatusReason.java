package com.xgen.mongot.index.status;

public enum RecoveringStatusReason implements StatusReason {
  VIEW_PIPELINE_ERROR(
      "View pipeline error: Replication encountered an error. Once the problem is fixed, indexing"
          + " will resume automatically. Please examine details and make changes to documents or"
          + " the view pipeline, if needed:%s"),
  RECOVERING(
      "Recovering: The index has encountered an issue and " + "is attempting automatic recovery.");
  private final String text;

  RecoveringStatusReason(String text) {
    this.text = text;
  }

  @Override
  public String formatMessage(Object... details) {
    return StatusReason.getString(this.text, details);
  }
}
