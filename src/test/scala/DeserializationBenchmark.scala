import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util

import demo.benchmark.{CurrentMapStatus, MutableScalaMapStatus, MutableTroveMapStatus, ProposedMapStatus}
import gnu.trove.map.hash.TIntByteHashMap
import org.roaringbitmap.RoaringBitmap
import org.scalameter.{Gen, PerformanceTest}
import org.scalameter.Key.exec

class DeserializationBenchmark extends PerformanceTest.Microbenchmark {

  val sizes: Gen[Int] = Gen.enumeration("total blocks")(100, 1000, 5000, 10000, 20000)
  val emptyPercentages: Gen[Double] = Gen.enumeration("empty blocks percentage")(0d, 0.15d, 0.5d, 0.75d)
  val currentMapByteArrays = for {
    size <- sizes
    ratio <- emptyPercentages
  } yield {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val rnd = new scala.util.Random(seed = 2018)
    val buffer = scala.collection.mutable.Map.empty[Int, Byte]
    val bitMap = new RoaringBitmap
    (0 until size).foreach { i =>
      if (rnd.nextDouble() > ratio) {
        buffer += i -> rnd.nextInt().toByte
      } else {
        bitMap.add(i)
      }
    }
    oos.writeObject(new CurrentMapStatus(emptyBlocks = bitMap, hugeBlockSizes = buffer.toMap))
    baos.toByteArray
  }

  val proposedMapByteArrays = for {
    size <- sizes
    ratio <- emptyPercentages
  } yield {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val rnd = new scala.util.Random(seed = 2018)
    val buffer = scala.collection.mutable.Map.empty[Int, Byte]
    val bitMap = new RoaringBitmap
    (0 until size).foreach { i =>
      if (rnd.nextDouble() > ratio) {
        buffer += i -> rnd.nextInt().toByte
      } else {
        bitMap.add(i)
      }
    }
    oos.writeObject(new ProposedMapStatus(emptyBlocks = bitMap, hugeBlockSizes = buffer.toMap))
    baos.toByteArray
  }

  val mutableMapByteArrays = for {
    size <- sizes
    ratio <- emptyPercentages
  } yield {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val rnd = new scala.util.Random(seed = 2018)
    val buffer = scala.collection.mutable.Map.empty[Int, Byte]
    val bitMap = new RoaringBitmap
    (0 until size).foreach { i =>
      if (rnd.nextDouble() > ratio) {
        buffer.put(i, rnd.nextInt().toByte)
      } else {
        bitMap.add(i)
      }
    }
    oos.writeObject(new MutableScalaMapStatus(emptyBlocks = bitMap, hugeBlockSizes = buffer))
    baos.toByteArray
  }

  val troveMapByteArrays = for {
    size <- sizes
    ratio <- emptyPercentages
  } yield {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val rnd = new scala.util.Random(seed = 2018)
    val buffer = new TIntByteHashMap()
    val bitMap = new RoaringBitmap
    (0 until size).foreach { i =>
      if (rnd.nextDouble() > ratio) {
        buffer.put(i, rnd.nextInt().toByte)
      } else {
        bitMap.add(i)
      }
    }
    oos.writeObject(new MutableTroveMapStatus(emptyBlocks = bitMap, hugeBlockSizes = buffer))
    baos.toByteArray
  }

  performance of "CurrentMapStatus version" config (exec.benchRuns -> 64) in {
    measure method "readExternal" in {
      using(currentMapByteArrays) in { barr  =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(barr))
        val obj = ois.readObject()
      }
    }
  }

  performance of "ProposedMapStatus version" in {
    measure method "readExternal" config (exec.benchRuns -> 64) in {
      using(proposedMapByteArrays) in { barr  =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(barr))
        val obj = ois.readObject()
      }
    }
  }
  performance of "MutableScalaMapStatus version" in {
    measure method "readExternal" config (exec.benchRuns -> 64) in {
      using(mutableMapByteArrays) in { barr  =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(barr))
        val obj = ois.readObject()
      }
    }
  }

  performance of "TroveMapStatus version" in {
    measure method "readExternal" config (exec.benchRuns -> 64) in {
      using(troveMapByteArrays) in { barr  =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(barr))
        val obj = ois.readObject()
      }
    }
  }



}
