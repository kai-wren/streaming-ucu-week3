package ua.edu.ucu

import java.util.concurrent.ForkJoinPool

import org.scalatest._
import ua.ucu.edu.{End, Node, Tree}
import ua.ucu.edu.ParTree._

class ParTreeSpec extends FlatSpec with Matchers{

  val simpleTree: Tree = Node(1,
    Node(2, End, End),
    Node(3, End, End) )

  val tree: Tree = Node(1,
    Node(2, Node(4, End, End), End),
    Node(3, End, Node(5, End, End)) )

  val complexTree: Tree = Node(1,
    Node(2,
      Node(4,
        Node(8,
          Node(16, End, End),
          Node(24, End, End)),
        Node(14,
          Node(17, End, End),
          Node(25, End, End))
      ),
      Node(6,
        Node(15,
          Node(18, End, End),
          Node(26, End, End)),
        Node(9,
          Node(19, End, End),
          Node(27, End, End))
      )
    ),
    Node(3,
      Node(7,
        Node(13,
          Node(20, End, End),
          Node(28, End, End)),
        Node(10,
          Node(21, End, End),
          Node(29, End, End))
      ),
      Node(5,
        Node(11,
          Node(22, End, End),
          Node(30, End, End)),
        Node(12,
          Node(23, End, End),
          Node(31, End, End))
      )
    )
  )

  val genTree:Tree = generateTree(20)

"A parallel tree sum" should "be equal to sum calculated via sequential recursive method and to the expected value" in {
  val parTest1 = new ForkJoinPool().submit(new ParTreeSum(simpleTree))
  parTest1.get() should be (simpleTree.sum())
  simpleTree.sum() should be (6)

  val parTest2 = new ForkJoinPool().submit(new ParTreeSum(tree))
  parTest2.get() should be (tree.sum())
  tree.sum() should be (15)

  val parTest3 = new ForkJoinPool().submit(new ParTreeSum(complexTree))
  parTest3.get() should be (complexTree.sum())
  complexTree.sum() should be (496)

  val parTest4 = new ForkJoinPool(4).submit(new ParTreeSum(genTree))
  parTest4.get() should be (genTree.sum())
  genTree.sum() should be (2097151)
}

}
