package ru.consultant.lakehouse.parser

import java.nio.charset.Charset

object ParserDemo {
  def main(args: Array[String]): Unit = {
    val path = args.headOption.getOrElse("/seed/0")
    val bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path))
    val content = new String(bytes, Charset.forName("Cp1251"))
    val sessionId = path.split("/").last
    val events = SessionParser.parse(sessionId, content)
    events.foreach(println)
    println(s"---- total events: ${events.size}")
  }
}
