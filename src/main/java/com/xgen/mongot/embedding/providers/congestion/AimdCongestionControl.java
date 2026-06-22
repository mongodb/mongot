package com.xgen.mongot.embedding.providers.congestion;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.bson.BsonDocument;

/**
 * An implementation of Additive Increase/Multiplicative Decrease (AIMD) congestion control modeled
 * on TCP.
 *
 * <p>AIMD works as follows:
 *
 * <ul>
 *   <li>The client maintains a congestion window (cwnd) that bounds in-flight work
 *   <li>On successful response: increase window by linear factor (additive increase)
 *   <li>On congestion signal (HTTP 429): decrease window by multiplicative factor
 *   <li>Slow start: allows faster ramp-up until threshold is reached
 *   <li>Idle timeout: returns to initial state after period of inactivity
 * </ul>
 *
 * <p>Reference: https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease
 */
@ThreadSafe
public class AimdCongestionControl implements DynamicSemaphorePolicy {

  /** Default initial congestion window size. */
  public static final int DEFAULT_INITIAL_CWND = 1;

  /** Default slow start threshold - clients can use multiplicative increase until this. */
  public static final int DEFAULT_SLOW_START_THRESHOLD = 16;

  /** Default linear increase factor per ACK. */
  public static final double DEFAULT_LINEAR_INCREASE = 1.0;

  /** Default multiplicative decrease factor (75% as per Flex Tier design, vs TCP's 50%). */
  public static final double DEFAULT_MULTIPLICATIVE_DECREASE = 0.75;

  /** Default idle timeout in milliseconds (5 minutes). */
  public static final int DEFAULT_IDLE_TIMEOUT_MS = 5 * 60 * 1000;

  /** Minimum congestion window size to prevent complete starvation. */
  public static final double MIN_CWND = 1.0;

  private final Object lock = new Object();

  private final int initialCwnd;
  private final int initialSlowStartThreshold;
  private final double linearIncrease;
  private final double multiplicativeDecrease;
  private final int idleTimeoutMillis;

  @GuardedBy("lock")
  private double cwnd;

  @GuardedBy("lock")
  private int slowStartThreshold;

  @GuardedBy("lock")
  private long sequenceNumber;

  @GuardedBy("lock")
  private long lastMultiplicativeDecreaseSequenceNumber;

  @GuardedBy("lock")
  private long lastActivityTimeMillis;

  /**
   * Creates an AimdCongestionControl with custom parameters.
   *
   * @param initialCwnd initial congestion window size (commonly 1)
   * @param slowStartThreshold threshold above which linear increase is used
   * @param linearIncrease additive increase factor per successful request
   * @param multiplicativeDecrease factor to multiply cwnd on congestion (e.g., 0.75)
   * @param idleTimeoutMillis time after which to reset to initial state
   */
  public AimdCongestionControl(
      int initialCwnd,
      int slowStartThreshold,
      double linearIncrease,
      double multiplicativeDecrease,
      int idleTimeoutMillis) {
    this.initialCwnd = initialCwnd;
    this.initialSlowStartThreshold = slowStartThreshold;
    this.linearIncrease = linearIncrease;
    this.multiplicativeDecrease = multiplicativeDecrease;
    this.idleTimeoutMillis = idleTimeoutMillis;
    this.cwnd = initialCwnd;
    this.slowStartThreshold = slowStartThreshold;
    this.sequenceNumber = 0;
    this.lastMultiplicativeDecreaseSequenceNumber = 0;
    this.lastActivityTimeMillis = 0;
  }

  /** Creates an AimdCongestionControl with default parameters. */
  public AimdCongestionControl() {
    this(
        DEFAULT_INITIAL_CWND,
        DEFAULT_SLOW_START_THRESHOLD,
        DEFAULT_LINEAR_INCREASE,
        DEFAULT_MULTIPLICATIVE_DECREASE,
        DEFAULT_IDLE_TIMEOUT_MS);
  }

  /**
   * Congestion control parameters for AIMD (config/BSON shape). Used by MMS {@code
   * autoEmbedding.congestionControl} and materialized view config. Optional fields use the same
   * defaults as {@link AimdCongestionControl}.
   */
  public record CongestionControlParams(
      int initialCwnd,
      int slowStartThreshold,
      double linearIncrease,
      double multiplicativeDecrease,
      int idleTimeoutMillis)
      implements DocumentEncodable {

    public static final class Fields {
      public static final Field.WithDefault<Integer> INITIAL_CWND =
          Field.builder("initialCwnd").intField().optional().withDefault(DEFAULT_INITIAL_CWND);
      public static final Field.WithDefault<Integer> SLOW_START_THRESHOLD =
          Field.builder("slowStartThreshold")
              .intField()
              .optional()
              .withDefault(DEFAULT_SLOW_START_THRESHOLD);
      public static final Field.WithDefault<Double> LINEAR_INCREASE =
          Field.builder("linearIncrease")
              .doubleField()
              .optional()
              .withDefault(DEFAULT_LINEAR_INCREASE);
      public static final Field.WithDefault<Double> MULTIPLICATIVE_DECREASE =
          Field.builder("multiplicativeDecrease")
              .doubleField()
              .optional()
              .withDefault(DEFAULT_MULTIPLICATIVE_DECREASE);
      public static final Field.WithDefault<Integer> IDLE_TIMEOUT_MS =
          Field.builder("idleTimeoutMillis")
              .intField()
              .optional()
              .withDefault(DEFAULT_IDLE_TIMEOUT_MS);
    }

    public static CongestionControlParams fromBson(DocumentParser parser)
        throws BsonParseException {
      return new CongestionControlParams(
          parser.getField(Fields.INITIAL_CWND).unwrap(),
          parser.getField(Fields.SLOW_START_THRESHOLD).unwrap(),
          parser.getField(Fields.LINEAR_INCREASE).unwrap(),
          parser.getField(Fields.MULTIPLICATIVE_DECREASE).unwrap(),
          parser.getField(Fields.IDLE_TIMEOUT_MS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.INITIAL_CWND, this.initialCwnd)
          .field(Fields.SLOW_START_THRESHOLD, this.slowStartThreshold)
          .field(Fields.LINEAR_INCREASE, this.linearIncrease)
          .field(Fields.MULTIPLICATIVE_DECREASE, this.multiplicativeDecrease)
          .field(Fields.IDLE_TIMEOUT_MS, this.idleTimeoutMillis)
          .build();
    }

    /**
     * Builds an {@link AimdCongestionControl} from this config, or default instance when {@code cc}
     * is empty.
     */
    public static AimdCongestionControl toAimdCongestionControl(
        Optional<CongestionControlParams> cc) {
      return cc.map(
              c ->
                  new AimdCongestionControl(
                      c.initialCwnd(),
                      c.slowStartThreshold(),
                      c.linearIncrease(),
                      c.multiplicativeDecrease(),
                      c.idleTimeoutMillis()))
          .orElseGet(AimdCongestionControl::new);
    }
  }

  /**
   * Builder for AimdCongestionControl to allow partial customization.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public int onAcquire() {
    synchronized (this.lock) {
      long ts = currentTimeMillis();
      if (this.lastActivityTimeMillis > 0
          && ts - this.lastActivityTimeMillis > this.idleTimeoutMillis) {
        this.cwnd = this.initialCwnd;
        this.slowStartThreshold = this.initialSlowStartThreshold;
      }
      this.lastActivityTimeMillis = ts;
      return getTotalPermits();
    }
  }

  @Override
  public int onRelease(boolean isAck) {
    synchronized (this.lock) {
      this.sequenceNumber++;
      if (isAck) {
        if (this.cwnd < this.slowStartThreshold) {
          // Slow start phase: exponential/multiplicative increase
          this.cwnd = this.cwnd + this.linearIncrease;
        } else {
          this.cwnd = this.cwnd + this.linearIncrease / this.cwnd;
        }
      } else {
        // Disable slow start by setting threshold to 0 after first congestion
        this.slowStartThreshold = 0;

        // Perform multiplicative decrease at most once per cwnd to avoid over-reaction
        if (this.sequenceNumber - this.lastMultiplicativeDecreaseSequenceNumber >= this.cwnd) {
          this.lastMultiplicativeDecreaseSequenceNumber = this.sequenceNumber;
          this.cwnd = Math.max(MIN_CWND, this.cwnd * this.multiplicativeDecrease);
        }
      }

      return getTotalPermits();
    }
  }

  @Override
  public int getTotalPermits() {
    synchronized (this.lock) {
      return (int) Math.ceil(this.cwnd);
    }
  }

  /**
   * Returns the current congestion window value (for metrics/debugging).
   *
   * @return the current cwnd value
   */
  public double getCwnd() {
    synchronized (this.lock) {
      return this.cwnd;
    }
  }

  /**
   * Returns the current slow start threshold (for metrics/debugging).
   *
   * @return the current slow start threshold
   */
  @VisibleForTesting
  int getSlowStartThreshold() {
    synchronized (this.lock) {
      return this.slowStartThreshold;
    }
  }

  /**
   * Returns the current time in milliseconds. Package-private for testing.
   *
   * @return current time in milliseconds
   */
  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  /** Builder for constructing AimdCongestionControl with custom parameters. */
  public static class Builder {
    private int initialCwnd = DEFAULT_INITIAL_CWND;
    private int slowStartThreshold = DEFAULT_SLOW_START_THRESHOLD;
    private double linearIncrease = DEFAULT_LINEAR_INCREASE;
    private double multiplicativeDecrease = DEFAULT_MULTIPLICATIVE_DECREASE;
    private int idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MS;

    private Builder() {}

    public Builder initialCwnd(int initialCwnd) {
      this.initialCwnd = initialCwnd;
      return this;
    }

    public Builder slowStartThreshold(int slowStartThreshold) {
      this.slowStartThreshold = slowStartThreshold;
      return this;
    }

    public Builder linearIncrease(double linearIncrease) {
      this.linearIncrease = linearIncrease;
      return this;
    }

    public Builder multiplicativeDecrease(double multiplicativeDecrease) {
      this.multiplicativeDecrease = multiplicativeDecrease;
      return this;
    }

    public Builder idleTimeoutMillis(int idleTimeoutMillis) {
      this.idleTimeoutMillis = idleTimeoutMillis;
      return this;
    }

    public AimdCongestionControl build() {
      return new AimdCongestionControl(
          this.initialCwnd,
          this.slowStartThreshold,
          this.linearIncrease,
          this.multiplicativeDecrease,
          this.idleTimeoutMillis);
    }
  }
}
