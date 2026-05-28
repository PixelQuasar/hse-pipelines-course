package ru.consultant.lakehouse.model

object EventType {
  val SessionStart = "SESSION_START"
  val SessionEnd   = "SESSION_END"
  val Qs           = "QS"
  val CardSearch   = "CARD_SEARCH"
  val DocOpen      = "DOC_OPEN"
  val Malformed    = "MALFORMED"
}

object SearchKind {
  val Qs   = "QS"
  val Card = "CARD"
}
