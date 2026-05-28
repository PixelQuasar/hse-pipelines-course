package ru.consultant.lakehouse.parser

import java.time.Instant
import ru.consultant.lakehouse.model.CardParam

sealed trait ParsedLine
object ParsedLine {
  final case class  SessionStart(ts: Instant)                              extends ParsedLine
  final case class  SessionEnd(ts: Instant)                                extends ParsedLine
  final case class  QsHeader(ts: Instant, query: String)                   extends ParsedLine
  final case class  CardSearchStart(ts: Instant)                           extends ParsedLine
  case object       CardSearchEnd                                          extends ParsedLine
  final case class  DocOpen(ts: Instant, searchId: String, docId: String)  extends ParsedLine
  final case class  DocOpenNoTs(searchId: String, docId: String)           extends ParsedLine
  final case class  CardParamLine(p: CardParam)                            extends ParsedLine
  final case class  SearchResults(searchId: String, docIds: Seq[String])   extends ParsedLine
  final case class  Malformed(raw: String, error: String)                  extends ParsedLine
}

object EventLineParser {
  import ParsedLine._

  private val SessionStartRx   = """^SESSION_START\s+(\S+)\s*$""".r
  private val SessionEndRx     = """^SESSION_END\s+(\S+)\s*$""".r
  private val QsRx             = """^QS\s+(\S+)\s+\{(.*)\}\s*$""".r
  private val CardStartRx      = """^CARD_SEARCH_START\s+(\S+)\s*$""".r
  private val DocOpenRx        = """^DOC_OPEN\s+(\S+)\s+(\S+)\s+(\S+)\s*$""".r
  // Some real-world DOC_OPEN lines come without a timestamp: "DOC_OPEN  <sid> <doc>".
  // Two-or-more-spaces between DOC_OPEN and sid → ts position is empty.
  private val DocOpenNoTsRx    = """^DOC_OPEN\s+(\S+)\s+(\S+)\s*$""".r
  private val CardParamRx      = """^\$(\S+)\s+(.+)$""".r
  // search_id may be negative (signed int overflow in source). Accept -?\d+.
  private val ResultsRx        = """^(-?\d+)((?:\s+\S+)*)\s*$""".r
  private val CardEndRx        = """^CARD_SEARCH_END$""".r

  def parse(line: String): ParsedLine = line match {
    case SessionStartRx(ts)         => withTs(ts, line, SessionStart.apply)
    case SessionEndRx(ts)           => withTs(ts, line, SessionEnd.apply)
    case QsRx(ts, query)            => withTs(ts, line, t => QsHeader(t, query))
    case CardStartRx(ts)            => withTs(ts, line, CardSearchStart.apply)
    case CardEndRx()                => CardSearchEnd
    case DocOpenRx(ts, sid, did)    => withTs(ts, line, t => DocOpen(t, sid, did))
    case DocOpenNoTsRx(sid, did)    => DocOpenNoTs(sid, did)
    case CardParamRx(pid, value)    => CardParamLine(CardParam(pid, value.trim))
    case ResultsRx(sid, docs)       => SearchResults(sid, docs.trim.split("""\s+""").filter(_.nonEmpty).toSeq)
    case other                      => Malformed(other, "unrecognized prefix")
  }

  private def withTs(s: String, raw: String, f: Instant => ParsedLine): ParsedLine =
    TimeParser.parse(s) match {
      case Some(t) => f(t)
      case None    => Malformed(raw, s"unparseable timestamp: $s")
    }
}
