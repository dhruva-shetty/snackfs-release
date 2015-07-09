package com.tuplejump.snackfs.server.messages

import com.tuplejump.snackfs.cassandra.sstable.SubBlockData

case class ReadSSTableResponse(data: SubBlockData) extends Serializable