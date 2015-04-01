package com.tuplejump.snackfs.util

import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface

import scala.collection.JavaConversions.enumerationAsScalaIterator

object NetworkHostUtil {

  var inetAddress: InetAddress = null
  
  def getHostAddress(): String = getInetAddress.getHostAddress
  
  def getInetAddress(): InetAddress = {
    if(inetAddress == null) {
      NetworkInterface.getNetworkInterfaces.flatMap { _.getInetAddresses }.foreach { address =>
        if (!address.isLoopbackAddress && address.isInstanceOf[Inet4Address]) {
          inetAddress = address
        }
      }
      if (inetAddress == null) throw new Exception("InetAddress lookup failed.")
    }
    inetAddress
  }
  
}