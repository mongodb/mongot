package com.xgen.mongot.server.command.registry;

import com.xgen.mongot.util.UserFacingException;

/**
 * Thrown when a request names a command that this mongot has not registered.
 *
 * <p>This is the normal outcome when a newer client (for example a mongod or mongos extension that
 * already knows a command this mongot release does not) talks to an older mongot during a rollout.
 * Servers map it to {@code CommandNotFound} so clients can tell "this command does not exist here"
 * apart from a failure of a command that does exist, without parsing the message.
 */
public class CommandNotFoundException extends IllegalArgumentException
    implements UserFacingException {

  private final String commandName;

  public CommandNotFoundException(String commandName) {
    super(messageFor(commandName));
    this.commandName = commandName;
  }

  public String getCommandName() {
    return this.commandName;
  }

  /** The message this exception always carries; unlike {@link #getMessage()} it is never null. */
  public String errorMessage() {
    return messageFor(this.commandName);
  }

  private static String messageFor(String commandName) {
    // The message is part of the contract: clients that predate the error code detect an unknown
    // command by matching this text, so it must not change.
    return "no command registered for " + commandName;
  }
}
