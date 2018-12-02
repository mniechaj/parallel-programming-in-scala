package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def loop(chars: Array[Char], acc: Int): Boolean = chars match {
      case Array() if acc == 0 => true
      case Array() => false
      case _ if acc < 0 => false
      case _ if chars.head == '(' => loop(chars.tail, acc + 1)
      case _ if chars.head == ')' => loop(chars.tail, acc - 1)
      case _ => loop(chars.tail, acc)
    }
    loop(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    * arg1 represents the degree of balance with '(' parentheses (how many unbalanced in traverse area)
    * arg2 represents the degree of balance with ')' parentheses (how many unbalanced in traverse area)
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      if (idx < until) {
        chars(idx) match {
          case '(' => traverse(idx + 1, until, arg1 + 1, arg2)
          case ')' =>
            if (arg1 > 0) traverse(idx + 1, until, arg1 - 1, arg2)
            else traverse(idx + 1, until, arg1, arg2 + 1)
          case _ => traverse(idx + 1, until, arg1, arg2)
        }
      } else (arg1, arg2)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val area = until - from
      if (area > threshold) {
        val half = from + (until - from) / 2
        val ((l1, l2), (r1, r2)) = parallel(reduce(from, half), reduce(half, until))
        if (l1 > r2) {
          (l1 - r2 + l1, l2)
        } else {
          (r1, r2 - l1 + l2)
        }
      } else {
        traverse(from, until, 0, 0)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

}
