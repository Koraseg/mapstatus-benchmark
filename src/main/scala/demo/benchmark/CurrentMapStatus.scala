package demo.benchmark

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable


/**
  * Cut version of HighlyCompressedMapStatus used in Apache Spark Core;
  * Only the heaviest parts, in terms of serialization, are kept.
  * @param emptyBlocks
  * @param hugeBlockSizes
  */
class CurrentMapStatus ( var emptyBlocks: RoaringBitmap,
                         var hugeBlockSizes: Map[Int, Byte]) extends Externalizable {

  protected def this() = this(null, null)  // For deserialization only


  override def writeExternal(out: ObjectOutput): Unit = {
    emptyBlocks.writeExternal(out)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    emptyBlocks = new RoaringBitmap
    emptyBlocks.readExternal(in)
    val count = in.readInt()
    val hugeBlockSizesArray = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesArray += Tuple2(block, size)
    }
    hugeBlockSizes = hugeBlockSizesArray.toMap
  }
}


/**
  * Cut version of HighlyCompressedMapStatus used in Apache Spark Core,
  * with proposed deserialization optimization;
  * Only the heaviest parts, in terms of serialization, are kept.
  * @param emptyBlocks
  * @param hugeBlockSizes
  */
class ProposedMapStatus (var emptyBlocks: RoaringBitmap,
                         var hugeBlockSizes: Map[Int, Byte]
                        ) extends Externalizable {
  protected def this() = this(null, null)  // For deserialization only

  override def writeExternal(out: ObjectOutput): Unit = {
    emptyBlocks.writeExternal(out)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    emptyBlocks = new RoaringBitmap
    emptyBlocks.readExternal(in)
    val count = in.readInt()
    var hugeBlockSizesMap = Map.empty[Int, Byte]
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesMap += (block -> size)
    }
    hugeBlockSizes = hugeBlockSizesMap

  }
}
