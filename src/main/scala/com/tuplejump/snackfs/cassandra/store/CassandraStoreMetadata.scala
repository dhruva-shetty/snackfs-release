package com.tuplejump.snackfs.cassandra.store

object CassandraStoreMetadata {
  
  //Cassandra Tables
  val INODE_TABLE = "inode"
  val SBLOCK_TABLE = "sblock"
  val LOCK_TABLE = "lock"

  //Cassandra columns
  val ID_COLUMN = "id"
  val PATH_COLUMN = "path"
  val PARENT_PATH_COLUMN = "parent_path"
  val SENTINEL_COLUMN = "sentinel"
  val DATA_COLUMN = "data"
  val MODIFIED_COLUMN = "modified"
  val BLOCK_ID_COLUMN = "block_id"
  val SBLOCK_ID_COLUMN = "sblock_id"
  val SENTINEL_VALUE = "x"
  val LOCK_ID_COLUMN = "lock_id"
  val PROCESS_ID_COLUMN = "process_id"

}