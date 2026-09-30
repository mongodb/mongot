package com.xgen.mongot.server.command.registry;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.server.command.builtin.PingCommand;
import com.xgen.mongot.util.UserFacingException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class CommandRegistryTest {

  @Test
  public void getCommandRegistration_registeredCommand_returnsRegistration() {
    CommandRegistry registry = CommandRegistry.create(new SimpleMeterRegistry());

    CommandRegistry.CommandRegistration registration =
        registry.getCommandRegistration(PingCommand.NAME);

    assertThat(registration.factory).isSameInstanceAs(PingCommand.FACTORY);
  }

  @Test
  public void getCommandRegistration_unknownCommand_throwsCommandNotFound() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    CommandRegistry registry = CommandRegistry.create(meterRegistry);

    CommandNotFoundException e =
        assertThrows(
            CommandNotFoundException.class,
            () -> registry.getCommandRegistration("materializeVectorSearch"));

    assertThat(e.getCommandName()).isEqualTo("materializeVectorSearch");
    // Message is part of the contract: clients on the old string-matching path rely on it.
    assertThat(e).hasMessageThat().isEqualTo("no command registered for materializeVectorSearch");
    assertThat(UserFacingException.isUserFacing(e)).isTrue();
    assertThat(meterRegistry.get("command.unknownCommands").counter().count()).isEqualTo(1.0);
  }
}
