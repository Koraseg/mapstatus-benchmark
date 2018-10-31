package demo.benchmark

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util
import gnu.trove.map.hash.TIntByteHashMap
import org.roaringbitmap.RoaringBitmap
import scala.collection.JavaConverters._
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


class MutableScalaMapStatus(var emptyBlocks: RoaringBitmap,
                            var hugeBlockSizes: scala.collection.mutable.Map[Int, Byte]
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
    hugeBlockSizes = new util.HashMap[Int, Byte](count).asScala

    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizes(block) = size
    }
  }
}

class MutableTroveMapStatus(var emptyBlocks: RoaringBitmap,
                            var hugeBlockSizes: TIntByteHashMap
                           ) extends Externalizable {
  protected def this() = this(null, null)  // For deserialization only

  override def writeExternal(out: ObjectOutput): Unit = {
    emptyBlocks.writeExternal(out)
    out.writeInt(hugeBlockSizes.size)
    val it = hugeBlockSizes.iterator()
    while (it.hasNext) {
      it.advance()
      out.writeInt(it.key())
      out.writeByte(it.value())
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    emptyBlocks = new RoaringBitmap
    emptyBlocks.readExternal(in)
    val count = in.readInt()
    hugeBlockSizes = new TIntByteHashMap(count)

    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizes.put(block, size)
    }
  }
}

