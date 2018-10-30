package com.sohu.news.sf

object Test {
  def main(args: Array[String]): Unit = {
    val a = List[Int](1, 2, 3)
    val b = List[Int](5, 7, 8)
    println(a.++(b))
  }
}
