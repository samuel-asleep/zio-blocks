package zio.blocks.schema

import zio.blocks.schema.json._
import scala.quoted._

package object json {
  extension (inline sc: StringContext) {
    inline def json(inline args: Any*): Json = ${ jsonInterpolatorImpl('sc, 'args) }
  }

  private def jsonInterpolatorImpl(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Expr[Json] = {
    import quotes.reflect._

    val parts = sc match {
      case '{ StringContext(${ Varargs(rawParts) }: _*) } =>
        rawParts.map { case '{ $rawPart: String } => rawPart.valueOrAbort }
      case _ => report.errorAndAbort("Expected a StringContext with string literal parts")
    }
    
    // Type-check each interpolation based on its context
    args match {
      case Varargs(argExprs) =>
        argExprs.zipWithIndex.foreach { case (arg, idx) =>
          val context = detectInterpolationContext(parts, idx)
          context match {
            case InterpolationContext.Key =>
              checkStringableType(arg, "key position")
            case InterpolationContext.Value =>
              checkHasJsonEncoder(arg, "value position")
            case InterpolationContext.StringLiteral =>
              checkStringableType(arg, "string literal")
          }
        }
      case _ => // No args to check
    }
    
    '{ JsonInterpolatorRuntime.jsonWithInterpolation($sc, $args) }
  }

  private enum InterpolationContext {
    case Key, Value, StringLiteral
  }

  private def detectInterpolationContext(parts: Seq[String], argIndex: Int): InterpolationContext = {
    val before = parts(argIndex)
    val after = if (argIndex + 1 < parts.length) parts(argIndex + 1) else ""
    
    // Check if we're inside a string literal (odd number of unescaped quotes before)
    if (isInStringLiteral(before)) {
      InterpolationContext.StringLiteral
    }
    // Check if this is a key position (after '{' or ',' and before ':')
    else if (isKeyPosition(before, after)) {
      InterpolationContext.Key
    }
    // Otherwise it's a value position
    else {
      InterpolationContext.Value
    }
  }

  private def isInStringLiteral(text: String): Boolean = {
    var inQuote = false
    var i = 0
    while (i < text.length) {
      val c = text.charAt(i)
      if (c == '"' && (i == 0 || text.charAt(i - 1) != '\\')) {
        inQuote = !inQuote
      }
      i += 1
    }
    inQuote
  }

  private def isKeyPosition(before: String, after: String): Boolean = {
    val trimmedBefore = before.reverse.dropWhile(c => c.isWhitespace).reverse
    val trimmedAfter = after.dropWhile(c => c.isWhitespace)
    
    (trimmedBefore.endsWith("{") || trimmedBefore.endsWith(",")) && trimmedAfter.startsWith(":")
  }

  private def checkStringableType(arg: Expr[Any], context: String)(using Quotes): Unit = {
    import quotes.reflect._
    
    val tpe = arg.asTerm.tpe.widen
    
    // Check if the type is a stringable primitive type
    val isStringable = tpe <:< TypeRepr.of[String] ||
      tpe <:< TypeRepr.of[Boolean] ||
      tpe <:< TypeRepr.of[Byte] ||
      tpe <:< TypeRepr.of[Short] ||
      tpe <:< TypeRepr.of[Int] ||
      tpe <:< TypeRepr.of[Long] ||
      tpe <:< TypeRepr.of[Float] ||
      tpe <:< TypeRepr.of[Double] ||
      tpe <:< TypeRepr.of[Char] ||
      tpe <:< TypeRepr.of[BigDecimal] ||
      tpe <:< TypeRepr.of[BigInt] ||
      tpe <:< TypeRepr.of[java.time.DayOfWeek] ||
      tpe <:< TypeRepr.of[java.time.Duration] ||
      tpe <:< TypeRepr.of[java.time.Instant] ||
      tpe <:< TypeRepr.of[java.time.LocalDate] ||
      tpe <:< TypeRepr.of[java.time.LocalDateTime] ||
      tpe <:< TypeRepr.of[java.time.LocalTime] ||
      tpe <:< TypeRepr.of[java.time.Month] ||
      tpe <:< TypeRepr.of[java.time.MonthDay] ||
      tpe <:< TypeRepr.of[java.time.OffsetDateTime] ||
      tpe <:< TypeRepr.of[java.time.OffsetTime] ||
      tpe <:< TypeRepr.of[java.time.Period] ||
      tpe <:< TypeRepr.of[java.time.Year] ||
      tpe <:< TypeRepr.of[java.time.YearMonth] ||
      tpe <:< TypeRepr.of[java.time.ZoneId] ||
      tpe <:< TypeRepr.of[java.time.ZoneOffset] ||
      tpe <:< TypeRepr.of[java.time.ZonedDateTime] ||
      tpe <:< TypeRepr.of[java.util.UUID] ||
      tpe <:< TypeRepr.of[java.util.Currency]
    
    if (!isStringable) {
      val typeStr = tpe.show
      report.errorAndAbort(
        s"Type error in JSON interpolation at $context:\n" +
        s"  Found: $typeStr\n" +
        s"  Required: A stringable type (primitive types as defined in PrimitiveType)\n" +
        s"  Hint: Only primitive types can be used in $context.\n" +
        s"        Supported types: String, Boolean, Byte, Short, Int, Long, Float, Double, Char,\n" +
        s"        BigDecimal, BigInt, java.time.*, java.util.UUID, java.util.Currency"
      )
    }
  }

  private def checkHasJsonEncoder(arg: Expr[Any], context: String)(using Quotes): Unit = {
    import quotes.reflect._
    
    val tpe = arg.asTerm.tpe.widen
    val encoderType = TypeRepr.of[JsonEncoder].appliedTo(tpe)
    
    Implicits.search(encoderType) match {
      case success: ImplicitSearchSuccess => // Has JsonEncoder, OK
      case failure: ImplicitSearchFailure =>
        val typeStr = tpe.show
        report.errorAndAbort(
          s"Type error in JSON interpolation at $context:\n" +
          s"  Found: $typeStr\n" +
          s"  Required: A type with an implicit JsonEncoder[$typeStr]\n" +
          s"  Hint: Provide an implicit JsonEncoder[$typeStr] in scope.\n" +
          s"        JsonEncoders can be:\n" +
          s"        - Explicitly defined\n" +
          s"        - Derived from Schema[$typeStr] (ensure implicit Schema[$typeStr] is in scope)\n" +
          s"        - Provided by JsonBinaryCodec"
        )
    }
  }
}
