package ua.ucu.edu

import java.util.concurrent.{ForkJoinPool, RecursiveTask}

import org.scalameter.measure

sealed trait Tree{
// non-parallel version of tree sum method
  def sum(res: Int=0): Int = {
    this match {
      case Node(v,l,r) => {
        v + l.sum(res) + r.sum(res)
      }
      case End => res
    }
  }

}
// branch of tree
case class Node(value: Int, left: Tree, right: Tree) extends Tree
// leaf of tree
case object End extends Tree

object ParTree extends App {
  // test tree
  val tree: Tree = Node(1,
    Node(2, Node(4, End, End), End),
    Node(3, End, Node(5, End, End)) )
  // big test tree created manually
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
  // simple test tree
  val simpleTree: Tree = Node(1,
    Node(2, End, End),
    Node(3, End, End) )
  //Measuring performance of 3 manually created tree on sequential sum method
  val time1seq = measure {
    val test1 = simpleTree.sum()
//    println(test1)
    val test2 = tree.sum()
//    println(test2)
    val test3 = complexTree.sum()
//    println(test3)
  }
  println("Summation (manual trees) executed sequentially in "+time1seq)
  println("**********")

  //defining parallel class to sum tree values using fork-join framework
  class ParTreeSum(tree: Tree, res: Int=0) extends RecursiveTask[Int]{
    override def compute(): Int = {
      tree match {
        case Node(v,l,r) => {
          val task1 = new ParTreeSum(l, res)
          val task2 = new ParTreeSum(r, res)
          task1.fork()
          v+task2.compute()+task1.join()
        }
        case End => res
      }
    }
  }
 // measuring execution of tree sum in parallel
  val time1par = measure {
    val parTest1 = new ForkJoinPool().submit(new ParTreeSum(simpleTree))
    parTest1.get()
//    println(parTest1.get())

    val parTest2 = new ForkJoinPool().submit(new ParTreeSum(tree))
    parTest2.get()
//    println(parTest2.get())

    val parTest3 = new ForkJoinPool().submit(new ParTreeSum(complexTree))
    parTest3.get()
//    println(parTest3.get())
  }
  println("Summation (manual trees) executed in parallel in "+time1par)
  println("**********")

  //method to generate test tree with value of 1 but with greater depth
  def generateTree(depth: Int): Tree = {
    depth match  {
      case 0 =>Node(1, End, End)
      case _ =>Node(1, generateTree(depth-1), generateTree(depth-1))
    }
  }
  // generating deep test tree
  val genTree = generateTree(20)
  // measuring sequential sum of generated tree
  val time2seq = measure {
  val test4 = genTree.sum()
//  println(test4)
  }
  println("Summation (generated tree) executed sequentially in "+time2seq)
  println("**********")
  //measuring parallel sum of generated tree
  val time2par = measure {
  val parTest4 = new ForkJoinPool(4).submit(new ParTreeSum(genTree))
  parTest4.get()
//  println(parTest4.get())
  }
  println("Summation (generated tree) executed in parallel in "+time2par)
  println("**********")

}
