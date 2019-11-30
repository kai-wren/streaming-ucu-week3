package ua.edu.ucu


import org.scalatest._
import ua.ucu.edu.ParCol._

import scala.collection.parallel.ParSeq

class ParColSpec extends FlatSpec with Matchers {

  val testStr:String = "Test are good! I like tests!"
  val parStr:ParString = new ParString(testStr)

  "A parallel string" should "works as usual sequence string" in {
    parStr.length should be (testStr.length)
    parStr.head should be (testStr.head)
    parStr.tail.seq.toString() should be (testStr.tail)
//    parStr.filter(_.toString == "!" ).seq.toString() should be (testStr.filter(_.toString == "!" ))

  }

}
