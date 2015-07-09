package com.tuplejump.snackfs

object SnackFSMode extends Enumeration {
  
  type SnackFSMode = Value
  
  val LOCAL, AKKA, NETTY = Value
  
  def isLocal(mode: SnackFSMode): Boolean = mode == LOCAL
  
  def isAkka(mode: SnackFSMode): Boolean = mode == AKKA
  
  def isNetty(mode: SnackFSMode): Boolean = mode == NETTY
}