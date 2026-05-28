package ru.consultant.lakehouse.model

import java.time.Instant

final case class RawEvent(
  sessionId:    String,
  eventSeq:     Int,
  eventTime:    Option[Instant],
  eventType:    String,
  searchId:     Option[String],
  searchKind:   Option[String],
  queryText:    Option[String],
  cardParams:   Seq[CardParam],
  resultDocIds: Seq[String],
  docId:        Option[String],
  parseError:   Option[String],
  rawLine:      Option[String]
)

object RawEvent {
  private def empty(sid: String, seq: Int): RawEvent =
    RawEvent(sid, seq, None, "", None, None, None, Nil, Nil, None, None, None)

  def sessionStart(sid: String, seq: Int, ts: Instant): RawEvent =
    empty(sid, seq).copy(eventTime = Some(ts), eventType = EventType.SessionStart)

  def sessionEnd(sid: String, seq: Int, ts: Instant): RawEvent =
    empty(sid, seq).copy(eventTime = Some(ts), eventType = EventType.SessionEnd)

  def qs(sid: String, seq: Int, ts: Instant, searchId: String, query: String, docs: Seq[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime    = Some(ts),
      eventType    = EventType.Qs,
      searchId     = Some(searchId),
      searchKind   = Some(SearchKind.Qs),
      queryText    = Some(query),
      resultDocIds = docs
    )

  def cardSearch(sid: String, seq: Int, ts: Instant, searchId: String,
                 params: Seq[CardParam], docs: Seq[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime    = Some(ts),
      eventType    = EventType.CardSearch,
      searchId     = Some(searchId),
      searchKind   = Some(SearchKind.Card),
      cardParams   = params,
      resultDocIds = docs
    )

  def docOpen(sid: String, seq: Int, ts: Instant, searchId: String, docId: String,
              resolvedKind: Option[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime  = Some(ts),
      eventType  = EventType.DocOpen,
      searchId   = Some(searchId),
      searchKind = resolvedKind,
      docId      = Some(docId)
    )

  /** DOC_OPEN without explicit timestamp: caller passes the last-seen ts from the same session. */
  def docOpenInheritedTs(sid: String, seq: Int, ts: Option[Instant], searchId: String, docId: String,
                         resolvedKind: Option[String]): RawEvent =
    empty(sid, seq).copy(
      eventTime  = ts,
      eventType  = EventType.DocOpen,
      searchId   = Some(searchId),
      searchKind = resolvedKind,
      docId      = Some(docId)
    )

  def malformed(sid: String, seq: Int, raw: String, err: String): RawEvent =
    empty(sid, seq).copy(
      eventType  = EventType.Malformed,
      parseError = Some(err),
      rawLine    = Some(raw)
    )
}
