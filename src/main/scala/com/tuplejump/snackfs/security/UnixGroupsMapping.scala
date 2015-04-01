package com.tuplejump.snackfs.security

import java.util.Collections
import java.util.HashSet
import java.util.Set
import org.apache.commons.collections.CollectionUtils
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping
import org.apache.commons.lang.SystemUtils

object UnixGroupsMapping {
  
  val unixGroup = if(SystemUtils.IS_OS_WINDOWS) null else new ShellBasedUnixGroupsMapping
  
  def getUserGroup(user: String): String = {
    if(unixGroup != null) {
      val groups = unixGroup.getGroups(user)
      if(!CollectionUtils.isEmpty(groups)) {
    	  groups.get(0) 
      }
    }
    user
  }
  
  def getUserGroups(user: String): Set[String] = {
    if(unixGroup != null) {
      val groups = unixGroup.getGroups(user)
      if(CollectionUtils.isEmpty(groups)) {
        new HashSet(groups)
      }
    }
    Collections.emptySet()
  }
}