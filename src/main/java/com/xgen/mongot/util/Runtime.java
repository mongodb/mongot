package com.xgen.mongot.util;

import com.google.errorprone.annotations.Var;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.Arrays;
import oshi.SystemInfo;
import oshi.software.os.CgroupInfo;

public abstract class Runtime {

  public static final Runtime INSTANCE = new RuntimeImpl();

  public abstract int getNumCpus();

  public abstract Bytes getMaxHeapSize();

  public abstract long getTotalMemoryBytes();

  public abstract void addShutdownHook(Runnable shutdownHook);

  public abstract void halt(int status);

  public abstract String getThreadDump();

  public abstract Instant getStartTime();

  private static class RuntimeImpl extends Runtime {

    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final java.lang.Runtime RUNTIME = java.lang.Runtime.getRuntime();

    private static final int MAX_STACK_SIZE_TO_FORMAT = 100;

    @Override
    public int getNumCpus() {
      return RUNTIME.availableProcessors();
    }

    @Override
    public Bytes getMaxHeapSize() {
      return Bytes.ofBytes(RUNTIME.maxMemory());
    }

    /**
     * Returns the instance's usable physical memory in bytes: the cgroup/container memory limit
     * when one is set below the host total, otherwise the host's total physical memory. This
     * mirrors how {@code MemoryMetrics} is computed.
     */
    @Override
    public long getTotalMemoryBytes() {
      var systemInfo = new SystemInfo();
      var globalMemory = systemInfo.getHardware().getMemory();
      var cgroupInfo = systemInfo.getOperatingSystem().getCgroupInfo();
      long hostTotal = globalMemory.getTotal();
      long cgroupLimit = cgroupInfo.getMemoryLimit();
      if (cgroupLimit < CgroupInfo.UNLIMITED_MEMORY && cgroupLimit < hostTotal) {
        return cgroupLimit;
      }
      return hostTotal;
    }

    @Override
    public void addShutdownHook(Runnable shutdownHook) {
      // If the shutdown hook throws an exception, simply shutdown immediately.
      Thread thread = new Thread(shutdownHook);

      RUNTIME.addShutdownHook(thread);
    }

    @Override
    public void halt(int status) {
      RUNTIME.halt(status);
    }

    @Override
    public String getThreadDump() {
      StringBuilder message = new StringBuilder();
      Arrays.stream(THREAD_MX_BEAN.dumpAllThreads(true, true))
          .map(this::dumpThreadInfo)
          .forEach(message::append);

      return message.toString();
    }

    private String dumpThreadInfo(ThreadInfo threadInfo) {
      // copied verbatim from ThreadInfo::toString with a minor change of not limiting the size of
      // the stack to 8.
      // https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/master/src/java.management/share/classes/java/lang/management/ThreadInfo.java#L597
      StringBuilder sb =
          new StringBuilder(
              String.format(
                  "\"%s\"%s prio=%d Id=%d %s",
                  threadInfo.getThreadName(),
                  threadInfo.isDaemon() ? " daemon" : "",
                  threadInfo.getPriority(),
                  threadInfo.getThreadId(),
                  threadInfo.getThreadState()));

      if (threadInfo.getLockName() != null) {
        sb.append(" on ").append(threadInfo.getLockName());
      }
      if (threadInfo.getLockOwnerName() != null) {
        sb.append(" owned by \"")
            .append(threadInfo.getLockOwnerName())
            .append("\" Id=")
            .append(threadInfo.getLockOwnerId());
      }
      if (threadInfo.isSuspended()) {
        sb.append(" (suspended)");
      }
      if (threadInfo.isInNative()) {
        sb.append(" (in native)");
      }
      sb.append('\n');

      StackTraceElement[] stackTrace = threadInfo.getStackTrace();
      Check.argNotNull(stackTrace, "stackTrace");
      for (@Var int i = 0; i < Integer.min(stackTrace.length, MAX_STACK_SIZE_TO_FORMAT); i++) {
        StackTraceElement ste = stackTrace[i];
        sb.append("\tat ").append(ste.toString());
        sb.append('\n');
        if (i == 0 && threadInfo.getLockInfo() != null) {
          Thread.State ts = threadInfo.getThreadState();
          switch (ts) {
            case BLOCKED -> {
              sb.append("\t-  blocked on ").append(threadInfo.getLockInfo());
              sb.append('\n');
            }
            case WAITING, TIMED_WAITING -> {
              sb.append("\t-  waiting on ").append(threadInfo.getLockInfo());
              sb.append('\n');
            }
            default -> {}
          }
        }

        for (MonitorInfo monitor : threadInfo.getLockedMonitors()) {
          if (monitor.getLockedStackDepth() == i) {
            sb.append("\t-  locked ").append(monitor);
            sb.append('\n');
          }
        }
      }

      LockInfo[] locks = threadInfo.getLockedSynchronizers();
      if (locks.length > 0) {
        sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
        sb.append('\n');
        for (LockInfo lock : locks) {
          sb.append("\t- ").append(lock);
          sb.append('\n');
        }
      }
      sb.append('\n');
      return sb.toString();
    }

    @Override
    public Instant getStartTime() {
      return Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime());
    }
  }
}
