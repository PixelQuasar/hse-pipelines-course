package ru.consultant.lakehouse.parser

import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

object TimeParser {
  private val Primary = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  // RFC-like format. Real data has both 'Fri,_12_Jun_...' and 'Tue,_2_Jun_...' —
  // use 'd' (1-2 digits) instead of 'dd' to accept both.
  private val Rfc     = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", Locale.ENGLISH)

  def parse(s: String): Option[Instant] =
    tryLocal(s, Primary).orElse(tryZoned(s, Rfc))

  private def tryLocal(s: String, fmt: DateTimeFormatter): Option[Instant] =
    try Some(LocalDateTime.parse(s, fmt).toInstant(ZoneOffset.UTC))
    catch { case _: Throwable => None }

  private def tryZoned(s: String, fmt: DateTimeFormatter): Option[Instant] =
    try Some(ZonedDateTime.parse(s, fmt).toInstant)
    catch { case _: Throwable => None }
}
