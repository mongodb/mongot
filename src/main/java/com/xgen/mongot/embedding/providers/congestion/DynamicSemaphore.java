package com.xgen.mongot.embedding.providers.congestion;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DynamicSemaphore is similar to a regular Semaphore with the added ability to vary the number of
 * available permits dynamically based on a policy (e.g., AIMD congestion control).
 *
 * <p>Unlike a standard Semaphore, the total number of permits can change based on feedback from
 * request outcomes, enabling adaptive flow control that responds to server congestion signals.
 */
@ThreadSafe
public class DynamicSemaphore {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicSemaphore.class);

  /** Result of a release operation containing current permit state. */
  public static class ReleaseResult {
    private final int totalPermits;
    private final int usedPermits;

    public ReleaseResult(int totalPermits, int usedPermits) {
      this.totalPermits = totalPermits;
      this.usedPermits = usedPermits;
    }

    public int getTotalPermits() {
      return this.totalPermits;
    }

    public int getUsedPermits() {
      return this.usedPermits;
    }
  }

  private final Object lock = new Object();

  @GuardedBy("lock")
  private int totalPermits;

  @GuardedBy("lock")
  private int usedPermits;

  private final DynamicSemaphorePolicy policy;

  /**
   * Creates a DynamicSemaphore with the specified policy.
   *
   * @param policy the policy used to determine the number of available permits
   */
  public DynamicSemaphore(DynamicSemaphorePolicy policy) {
    this.totalPermits = policy.getTotalPermits();
    this.usedPermits = 0;
    this.policy = policy;
  }

  /**
   * Acquires a permit. If no permits are available, the thread blocks until one is released.
   *
   * @return the number of permits currently in use after acquisition
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public int acquire() throws InterruptedException {
    synchronized (this.lock) {
      this.totalPermits = this.policy.onAcquire();
      while (this.usedPermits >= this.totalPermits) {
        this.lock.wait();
      }
      return ++this.usedPermits;
    }
  }

  /**
   * Attempts to acquire a permit without blocking.
   *
   * @return the number of permits in use if successful, -1 if no permits available
   */
  public int tryAcquire() {
    synchronized (this.lock) {
      this.totalPermits = this.policy.onAcquire();
      if (this.usedPermits < this.totalPermits) {
        return ++this.usedPermits;
      }
      return -1;
    }
  }

  /**
   * Releases a permit, returning it to the semaphore and signaling waiting threads.
   *
   * @param isAck true if the request was successful (no congestion), false if a congestion signal
   *     was received
   * @return the result containing the updated permit counts
   */
  public ReleaseResult release(boolean isAck) {
    synchronized (this.lock) {
      if (this.usedPermits <= 0) {
        LOG.warn(
            "Cannot release permit: no permits are currently in use (usedPermits="
                + this.usedPermits
                + ")");
        return new ReleaseResult(this.totalPermits, this.usedPermits);
      }
      this.usedPermits--;
      this.totalPermits = this.policy.onRelease(isAck);

      if (this.usedPermits < this.totalPermits) {
        this.lock.notifyAll();
      }
      return new ReleaseResult(this.totalPermits, this.usedPermits);
    }
  }

  /**
   * Releases a permit, returning it to the semaphore and signaling waiting threads. Don't apply the
   * policy, just release the permit
   *
   * @return the result containing the updated permit counts
   */
  public ReleaseResult release() {
    synchronized (this.lock) {
      if (this.usedPermits <= 0) {
        LOG.warn(
            "Cannot release permit: no permits are currently in use (usedPermits="
                + this.usedPermits
                + ")");
        return new ReleaseResult(this.totalPermits, this.usedPermits);
      }
      this.usedPermits--;
      this.totalPermits = this.policy.getTotalPermits();

      if (this.usedPermits < this.totalPermits) {
        this.lock.notifyAll();
      }
      return new ReleaseResult(this.totalPermits, this.usedPermits);
    }
  }

  /**
   * Returns the current total number of permits available.
   *
   * @return the total number of permits
   */
  public int getTotalPermits() {
    synchronized (this.lock) {
      return this.totalPermits;
    }
  }

  /**
   * Returns the current number of permits in use.
   *
   * @return the number of permits currently in use
   */
  public int getUsedPermits() {
    synchronized (this.lock) {
      return this.usedPermits;
    }
  }

  /**
   * Returns the number of available permits.
   *
   * @return the number of permits available for acquisition
   */
  @VisibleForTesting
  int getAvailablePermits() {
    synchronized (this.lock) {
      return Math.max(0, this.totalPermits - this.usedPermits);
    }
  }
}
