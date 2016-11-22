package com.twitter.storehaus.postgres

/**
  * Exception for ibvalid conversions
  *
  * @author Alexey Ponkin
  */
case class IllegalConversionException(msg: String) extends Exception
