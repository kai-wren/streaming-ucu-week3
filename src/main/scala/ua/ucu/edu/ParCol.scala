package ua.ucu.edu

import ua.ucu.edu.ParCol.ParString

import scala.collection.mutable
import scala.collection.parallel.immutable.ParSeq
import scala.collection.parallel.{Combiner, SeqSplitter, immutable}

object ParCol {

  class ParString(val str: String)
    extends immutable.ParSeq[Char]{
    //method returns Character for given index i
    def apply(i: Int): Char = str.charAt(i)
    //methods return total string length
    def length: Int = str.length
    //method to return sequential version of the string
    def seq = new collection.immutable.WrappedString(str)
    //splitter to be used by Parallel String
    def splitter: SeqSplitter[Char] = new ParStringSplitter(str, 0, str.length)
    //combiner to be used by Parallel String to receive ParString as result
    override def newCombiner: Combiner[Char, ParSeq[Char]] = new ParStringCombiner

  }
  //defining splitter for parallel string
  class ParStringSplitter(var str: String, var curPos: Int, val totLen: Int)
    extends SeqSplitter[Char] {
    // method to check whether or not there is next element
    final def hasNext: Boolean = curPos < totLen
    // method to return next element
    final def next: Char = {
      val res = str.charAt(curPos)
      curPos += 1
      res
    }
    // method to return amount of elements left to iterate
    def remaining: Int = totLen - curPos
    // method co create copy of splitter
    def dup = new ParStringSplitter(str, curPos, totLen)
    // method to split splitter into sequence of splitters using another method psplit
    def split: Seq[ParStringSplitter] = {
      val rem = remaining
      if (rem >= 3) psplit(rem / 3, 2*rem / 3 - rem / 3, rem - 2*rem / 3)
      else Seq(this)
    }
    // method to split splitter into sequence of splitters
    def psplit(sizes: Int*): Seq[ParStringSplitter] = {
      val splitted = new mutable.MutableList[ParStringSplitter]
      for (sizeSingle <- sizes) {
        val next = (curPos + sizeSingle) min totLen
        splitted += new ParStringSplitter(str, curPos, next)
        curPos = next
      }
      if (remaining > 0) splitted += new ParStringSplitter(str, curPos, totLen)
      splitted
    }

  }
  //defining combiner for parallel string
  class ParStringCombiner extends Combiner[Char, ParString] {
    var curSize: Int = 0
    val builders: mutable.MutableList[StringBuilder] = new mutable.MutableList[StringBuilder] += new StringBuilder
    var lastBuilder: StringBuilder = builders.last
    // method to return current number of elements combined
    def size: Int = curSize
    // method to add single element to the combiner
    def +=(elem: Char): this.type = {
      lastBuilder += elem
      curSize += 1
      this
    }
    // method to remove content of combiner
    def clear: Unit = {
      builders.clear
      builders += new StringBuilder
      lastBuilder = builders.last
      curSize = 0
    }
    // method to return collection of elements
    def result: ParString = {
      val rsb = new StringBuilder
      for (sb <- builders) rsb.append(sb)
      new ParString(rsb.toString)
    }
    // method to merge content of two combiners together into single one
    def combine[C <: Char, To >: ParString](other: Combiner[C, To]): ParStringCombiner = if (other eq this) this else {
      val that = other.asInstanceOf[ParStringCombiner]
      curSize += that.curSize
      builders ++= that.builders
      lastBuilder = builders.last
      this
    }
  }

}
//checking result using sequential string and parallel string
object CustomCharCount extends App {
  val txt = "A custom text " * 250000
  val partxt = new ParString(txt)

  val seqtime = warmedTimed(50) {
    txt.foldLeft(0)((n, c) => if (Character.isUpperCase(c)) n + 1 else n)
  }

  println(s"Sequential time - $seqtime ms")

  val partime = warmedTimed(50) {
    partxt.aggregate(0)((n, c) => if (Character.isUpperCase(c)) n + 1 else n, _ + _)
  }

  println(s"Parallel time   - $partime ms")

  val testStr:String = "Test are good! I like tests!"
  val parStr:ParString = new ParString(testStr)
//  println(parStr.map(_ == '!'))
//  println(parStr.max)
}