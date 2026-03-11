package com.xgen.mongot.util.mongodb;

// A unique signature per replica set that is not reused upon shard removal and recreation.
public record ReplicaSetSignature(String rsId, String createDate) {}
