package com.xgen.mongot.replication.mongodb.common;

/** ClientSessionRecord holds the synchronous MongoClient and the corresponding session refresher */
public record ClientSessionRecord(
    com.mongodb.client.MongoClient syncMongoClient, SessionRefresher sessionRefresher) {}
