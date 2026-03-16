package com.xgen.mongot.server.command.search;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.MongotCursorManager;
import java.util.List;

public class CursorGuard implements AutoCloseable {
  private final List<Long> createdCursorIds;
  private final MongotCursorManager cursorManager;
  @Var private boolean keepCursors = false;

  CursorGuard(List<Long> createdCursorIds, MongotCursorManager cursorManager) {
    this.createdCursorIds = createdCursorIds;
    this.cursorManager = cursorManager;
  }

  /**
   * Keeps created cursors on close(). Call only after the command has successfully built the
   * response and wants ownership transferred to the client.
   */
  void keepCursors() {
    this.keepCursors = true;
  }

  @Override
  public void close() {
    if (!this.keepCursors) {
      this.createdCursorIds.forEach(this.cursorManager::killCursor);
    }
  }
}
