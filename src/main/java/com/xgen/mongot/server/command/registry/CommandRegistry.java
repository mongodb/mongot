package com.xgen.mongot.server.command.registry;

import com.google.common.base.CaseFormat;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.server.command.CommandFactoryMarker;
import com.xgen.mongot.server.command.builtin.BuildInfoCommand;
import com.xgen.mongot.server.command.builtin.HelloCommand;
import com.xgen.mongot.server.command.builtin.IsMasterCommand;
import com.xgen.mongot.server.command.builtin.PingCommand;
import com.xgen.mongot.util.Enums;
import com.xgen.mongot.util.ErrorType;
import com.xgen.mongot.util.UserFacingException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CommandRegistry {

  private final Map<String, CommandRegistration> commands;
  private final MetricsFactory commandMetricsFactory;

  private CommandRegistry(MetricsFactory commandMetricsFactory) {
    this.commands = new ConcurrentHashMap<>();
    this.commandMetricsFactory = commandMetricsFactory;
  }

  public static CommandRegistry create(MeterRegistry meterRegistry) {
    MetricsFactory metricsFactory = new MetricsFactory("command", meterRegistry);
    CommandRegistry commandRegistry = new CommandRegistry(metricsFactory);
    registerBuiltInCommands(commandRegistry);
    return commandRegistry;
  }

  private static void registerBuiltInCommands(CommandRegistry commandRegistry) {
    commandRegistry.registerInsecureCommand(
        BuildInfoCommand.ALT_NAME, BuildInfoCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(HelloCommand.NAME, HelloCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(IsMasterCommand.NAME, IsMasterCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(
        IsMasterCommand.ALT_NAME, IsMasterCommand.FACTORY, false);
    commandRegistry.registerInsecureCommand(PingCommand.NAME, PingCommand.FACTORY, false);
  }

  public CommandRegistry registerCommand(
      String commandName, CommandFactoryMarker factory, boolean detailedStats) {
    this.commands.put(
        commandName,
        new CommandRegistration(
            commandName, factory, true, this.commandMetricsFactory, detailedStats));
    return this;
  }

  public CommandRegistry registerInsecureCommand(
      String commandName, CommandFactoryMarker factory, boolean detailedStats) {
    this.commands.put(
        commandName,
        new CommandRegistration(
            commandName, factory, false, this.commandMetricsFactory, detailedStats));
    return this;
  }

  public CommandRegistration getCommandRegistration(String commandName) {
    CommandRegistration reg = this.commands.get(commandName);
    if (reg == null) {
      throw new IllegalArgumentException("no command registered for " + commandName);
    }
    return reg;
  }

  public static class CommandRegistration {
    public final CommandFactoryMarker factory;
    public final boolean isSecure;
    public final Timer totalTimer;
    public final Optional<Timer> serializationTimer;
    private final MetricsFactory commandMetricsFactory;
    private final String failureCounterName;

    private CommandRegistration(
        String commandName,
        CommandFactoryMarker factory,
        boolean isSecure,
        MetricsFactory commandMetricsFactory,
        boolean detailedStats) {
      this.factory = factory;
      this.isSecure = isSecure;
      this.commandMetricsFactory = commandMetricsFactory;
      this.failureCounterName = String.format("%sCommandFailure", commandName);
      // To avoid triggering lookups for each command, the metric is cached in this class.
      if (detailedStats) {
        this.totalTimer =
            commandMetricsFactory.timerSec(String.format("%sCommandTotalLatency", commandName));
        this.serializationTimer =
            Optional.of(
                commandMetricsFactory.timer(
                    String.format("%sCommandSerializationLatency", commandName),
                    Tags.of(
                        "timeUnit",
                        Enums.convertNameTo(CaseFormat.LOWER_CAMEL, TimeUnit.MICROSECONDS)),
                    0.5,
                    0.75,
                    0.9,
                    0.99));
      } else {
        // NB: no percentiles
        this.totalTimer =
            commandMetricsFactory.timer(
                String.format("%sCommandTotalLatency", commandName), Tags.empty());
        this.serializationTimer = Optional.empty();
      }
    }

    /** Emit failure metrics with the same set of label keys. */
    public void recordFailure(Throwable throwable) {
      ErrorType errorType =
          UserFacingException.isUserFacing(throwable)
              ? ErrorType.USER_FACING_ERROR
              : ErrorType.INTERNAL_ERROR;
      this.commandMetricsFactory
          .counter(
              this.failureCounterName,
              Tags.of(
                  "exceptionName",
                  throwable.getClass().getSimpleName(),
                  ErrorType.METRIC_TAG_KEY,
                  errorType.toString()))
          .increment();
    }
  }
}
