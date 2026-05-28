package ru.consultant.lakehouse.parser

import ru.consultant.lakehouse.model.{CardParam, RawEvent, SearchKind}
import java.time.Instant

object SessionParser {

  private sealed trait State
  private object State {
    final case object Neutral                                              extends State
    final case class AwaitingQsResults(ts: Instant, query: String)         extends State
    final case class InCardSearch(
      ts:              Instant,
      params:          Vector[CardParam],
      awaitingResults: Boolean = false
    ) extends State
  }

  def parse(sessionId: String, content: String): Seq[RawEvent] = {
    val out  = scala.collection.mutable.ArrayBuffer.empty[RawEvent]
    var state: State = State.Neutral
    var seq  = 0
    val kindBySearchId = scala.collection.mutable.HashMap.empty[String, String]
    var lastTs: Option[Instant] = None

    content.linesIterator.foreach { raw =>
      val line = raw.trim
      if (line.nonEmpty) {
        val (emitted, newState) = step(state, EventLineParser.parse(line), line, sessionId, seq, kindBySearchId, lastTs)
        emitted.foreach { ev =>
          out += ev
          ev.eventTime.foreach(t => lastTs = Some(t))
          seq += 1
        }
        state = newState
      }
    }
    out.toSeq
  }

  private def step(
    state:          State,
    parsed:         ParsedLine,
    raw:            String,
    sessionId:      String,
    seq:            Int,
    kindBySearchId: scala.collection.mutable.HashMap[String, String],
    lastTs:         Option[Instant]
  ): (Seq[RawEvent], State) = {
    import ParsedLine._

    (state, parsed) match {
      case (_, SessionStart(ts)) =>
        (Seq(RawEvent.sessionStart(sessionId, seq, ts)), State.Neutral)
      case (_, SessionEnd(ts)) =>
        (Seq(RawEvent.sessionEnd(sessionId, seq, ts)), State.Neutral)

      case (_, QsHeader(ts, query)) =>
        (Nil, State.AwaitingQsResults(ts, query))
      case (State.AwaitingQsResults(ts, q), SearchResults(sid, docs)) =>
        kindBySearchId.update(sid, SearchKind.Qs)
        (Seq(RawEvent.qs(sessionId, seq, ts, sid, q, docs)), State.Neutral)

      case (_, CardSearchStart(ts)) =>
        (Nil, State.InCardSearch(ts, Vector.empty))
      case (s @ State.InCardSearch(_, ps, false), CardParamLine(p)) =>
        (Nil, s.copy(params = ps :+ p))
      case (s @ State.InCardSearch(_, _, false), CardSearchEnd) =>
        (Nil, s.copy(awaitingResults = true))
      case (State.InCardSearch(ts, ps, true), SearchResults(sid, docs)) =>
        kindBySearchId.update(sid, SearchKind.Card)
        (Seq(RawEvent.cardSearch(sessionId, seq, ts, sid, ps, docs)), State.Neutral)

      case (st, DocOpen(ts, sid, did)) =>
        val kind = kindBySearchId.get(sid)
        (Seq(RawEvent.docOpen(sessionId, seq, ts, sid, did, kind)), st)

      case (st, DocOpenNoTs(sid, did)) =>
        // Inherit the last seen event_time so this row still partitions/filters by date.
        val kind = kindBySearchId.get(sid)
        (Seq(RawEvent.docOpenInheritedTs(sessionId, seq, lastTs, sid, did, kind)), st)

      case (st, Malformed(_, err)) =>
        (Seq(RawEvent.malformed(sessionId, seq, raw, err)), st)

      case (st, unexpected) =>
        val err = s"unexpected '${unexpected.getClass.getSimpleName}' in state ${st.getClass.getSimpleName}"
        (Seq(RawEvent.malformed(sessionId, seq, raw, err)), st)
    }
  }
}
