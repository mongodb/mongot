package com.xgen.mongot.server.command.search;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.request.KillCursorsCommandDefinition;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillCursorsCommandTest {

  private static final long MOCK_CURSOR_ID = 12345L;

  @Test
  public void maybeLoadShed_returnsFalse() {
    MongotCursorManager cursorManager = mock(MongotCursorManager.class);

    BsonDocument mockArgs =
        new BsonDocument()
            .append(KillCursorsCommandDefinition.NAME, new BsonString("test"))
            .append("cursors", new BsonArray(List.of(new BsonInt64(MOCK_CURSOR_ID))));

    CommandFactory commandFactory = new KillCursorsCommand.Factory(cursorManager);
    Command command = commandFactory.create(mockArgs);

    Assert.assertFalse(
        "KillCursorsCommand should not be load shed to ensure cursor cleanup always succeeds",
        command.maybeLoadShed());
  }
}
