package com.twitter.storehaus.cascading.split

import com.twitter.concurrent.Spool
import java.io.{ DataInput, DataOutput }
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputSplit
import scala.reflect.runtime.universe._

class SplittableStoreInputSplit[K, V, Q <: Writable](var tapid: String, var splitParams: Q, var version: Option[Long]) extends InputSplit {

  @transient var spool: Option[Spool[(K, V)]] = None
  
  def this() = this(null, null.asInstanceOf[Q], None)
  
  override def getLocations: Array[String] = Array[String]()
  override def getLength: Long = 0l
  
  def setPredicate(predicate: Q): SplittableStoreInputSplit[K, V, Q] = { 
    splitParams = predicate
    this 
  }
  def getPredicate: Q = splitParams
  
  override def write(out: DataOutput): Unit = {
	out.writeUTF(tapid)
	out.writeBoolean(version != None)
	if(version != None) {
	  out.writeLong(version.get)
	}
    out.writeUTF(splitParams.getClass.getName)
    splitParams.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    tapid = in.readUTF()
    if(in.readBoolean) version = Some(in.readLong)
    val classname = in.readUTF()
    val loadermirror = runtimeMirror(getClass.getClassLoader)
    val cm = getReflectiveClass(classname)
	val methods = cm.symbol.typeSignature.member(nme.CONSTRUCTOR).asTerm.alternatives
	val method = methods.find(cstr => cstr.asMethod.paramss(0).size == 0)
	val constr = cm.reflectConstructor(method.get.asMethod)
	splitParams = constr().asInstanceOf[Q]
    splitParams.readFields(in)
  }
  
  def getReflectiveClass(className: String) = {
    val loadermirror = runtimeMirror(getClass.getClassLoader)
	val module = loadermirror.staticClass(className)
	loadermirror.reflectClass(module)
  }
}
