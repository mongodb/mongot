package com.xgen.mongot.monitor;

import java.time.Instant;

/** Gate can be opened or closed to block certain processes. */
public interface Gate {
  /**
   * Whether the gate is open or not.
   *
   * @return true if the gate is open
   */
  boolean isOpen();

  /**
   * Whether the gate is closed or not.
   *
   * @return true if the gate is closed
   */
  default boolean isClosed() {
    return !isOpen();
  }

  /**
   * Blocks until the gate is open. This returns immediately if the gate is already open.
   *
   * @throws InterruptedException if the thread was interrupted before the gate opens.
   */
  void awaitOpen() throws InterruptedException;

  /**
   * Blocks until the gate is closed. This returns immediately if the gate is already closed.
   *
   * @throws InterruptedException if the thread was interrupted before the gate closes.
   */
  void awaitClose() throws InterruptedException;

  /**
   * Updates the gate according to the incoming value.
   *
   * @param value latest observed value
   */
  void update(double value);

  Instant lastChanged();
}
