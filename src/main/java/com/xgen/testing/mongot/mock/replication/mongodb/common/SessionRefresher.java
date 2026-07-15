package com.xgen.testing.mongot.mock.replication.mongodb.common;

import static org.mockito.Mockito.spy;

import com.mongodb.session.ClientSession;
import com.xgen.mongot.replication.mongodb.common.RefreshingClientSession;

public class SessionRefresher {

  public static com.xgen.mongot.replication.mongodb.common.SessionRefresher mockSessionRefresher() {
    return spy(new NoopSessionRefresher());
  }

  private static class NoopSessionRefresher
      implements com.xgen.mongot.replication.mongodb.common.SessionRefresher {

    @Override
    public <S extends ClientSession> RefreshingClientSession<S> register(S session) {
      return new RefreshingClientSession<>() {
        @Override
        public S getSession() {
          return session;
        }

        @Override
        public void close() {
          session.close();
        }
      };
    }

    @Override
    public void markStopping() {}

    @Override
    public void shutdown() {}
  }
}
