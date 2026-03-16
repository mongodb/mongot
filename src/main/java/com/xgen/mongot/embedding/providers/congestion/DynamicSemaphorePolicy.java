package com.xgen.mongot.embedding.providers.congestion;

/**
 * A DynamicSemaphorePolicy provides the ability to vary the number of available permits in a
 * DynamicSemaphore. This interface enables implementing different congestion control algorithms
 * such as AIMD (Additive Increase Multiplicative Decrease).
 */
public interface DynamicSemaphorePolicy {

  /**
   * A callback that is invoked when a permit is acquired.
   *
   * @return the updated total number of permits available.
   */
  int onAcquire();

  /**
   * A callback that is invoked when a permit is released.
   *
   * @param isAck true if the release is due to a successful request (i.e., no congestion occurred),
   *     false if the request received a congestion signal.
   * @return the updated total number of permits available.
   */
  int onRelease(boolean isAck);

  /**
   * Returns the current total number of permits available.
   *
   * @return the total number of permits
   */
  int getTotalPermits();
}
