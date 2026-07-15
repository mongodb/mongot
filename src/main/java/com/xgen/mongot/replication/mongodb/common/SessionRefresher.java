package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.session.ClientSession;

public interface SessionRefresher {

  /**
   * Registers the supplied session with the SessionRefresher to be periodically refreshed.
   *
   * <p>Returns a RefreshingClientSession that wraps and owns the supplied session. When the
   * RefreshingClientSession is close()-ed, the session will be de-registered from being refreshed,
   * and the underlying session will be closed.
   */
  <S extends ClientSession> RefreshingClientSession<S> register(S session);

  /**
   * Signals that the shared MongoClient is about to be (or has been) closed as part of replication
   * shutdown. After this is called, refresh failures caused by the closed client are expected and
   * are ignored rather than being logged as errors or counted as failed refreshes.
   */
  void markStopping();

  void shutdown();
}
