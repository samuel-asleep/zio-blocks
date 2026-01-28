package zio.blocks.schema.json

import zio.blocks.chunk.Chunk
import zio.blocks.schema._
import zio.blocks.schema.patch.{Differ, DynamicPatch}

/**
 * An untyped patch that operates on [[Json]] values.
 *
 * JsonPatch mirrors [[zio.blocks.schema.patch.DynamicPatch]] while specializing
 * operations to JSON's simpler data model. JsonPatch operations are serializable,
 * composable, and diffable.
 *
 * ==Example==
 *
 * {{{
 * val source = Json.Object("name" -> Json.String("Alice"), "age" -> Json.Number("30"))
 * val target = Json.Object("name" -> Json.String("Bob"), "age" -> Json.Number("31"))
 *
 * val patch = JsonPatch.diff(source, target)
 * val result = patch(source, JsonPatchMode.Strict)
 * // Right(target)
 * }}}
 *
 * ==Algebraic Laws==
 *
 * '''Monoid Laws''' (under `++` composition):
 * {{{
 * // 1. LEFT IDENTITY
 * ∀ p: JsonPatch, j: Json.
 *   (JsonPatch.empty ++ p)(j, mode) == p(j, mode)
 *
 * // 2. RIGHT IDENTITY
 * ∀ p: JsonPatch, j: Json.
 *   (p ++ JsonPatch.empty)(j, mode) == p(j, mode)
 *
 * // 3. ASSOCIATIVITY
 * ∀ p1, p2, p3: JsonPatch, j: Json.
 *   ((p1 ++ p2) ++ p3)(j, mode) == (p1 ++ (p2 ++ p3))(j, mode)
 * }}}
 *
 * '''Diff/Apply Laws''':
 * {{{
 * // 4. ROUNDTRIP
 * ∀ source, target: Json.
 *   JsonPatch.diff(source, target)(source, JsonPatchMode.Strict) == Right(target)
 *
 * // 5. IDENTITY DIFF
 * ∀ j: Json.
 *   JsonPatch.diff(j, j).isEmpty == true
 *
 * // 6. DIFF COMPOSITION
 * ∀ a, b, c: Json.
 *   (JsonPatch.diff(a, b) ++ JsonPatch.diff(b, c))(a, JsonPatchMode.Strict) == Right(c)
 * }}}
 *
 * '''PatchMode Law''':
 * {{{
 * // 7. LENIENT SUBSUMES STRICT
 * ∀ p: JsonPatch, j: Json.
 *   p(j, Strict) == Right(r) implies p(j, Lenient) == Right(r)
 * }}}
 *
 * ==Relation to DynamicPatch==
 *
 * JsonPatch provides a JSON-native patch algebra while allowing bidirectional
 * conversion to [[zio.blocks.schema.patch.DynamicPatch]]. The conversion uses
 * a JSON-shaped DynamicValue representation: objects become maps with string
 * keys, arrays become sequences, and numbers become BigDecimal primitives.
 *
 * @param ops The sequence of patch operations
 */
final case class JsonPatch(ops: Vector[JsonPatch.JsonPatchOp]) {

  /**
   * Applies this patch to a JSON value.
   *
   * @param json The JSON value to patch
   * @param mode The patch mode (default: Strict)
   * @return Either an error or the patched value
   */
  def apply(json: Json, mode: JsonPatchMode = JsonPatchMode.Strict): Either[JsonError, Json] = {
    var current                    = json
    var idx                        = 0
    var error: Either[JsonError, Unit] = Right(())

    while (idx < ops.length && error.isRight) {
      val op = ops(idx)
      JsonPatch.applyOp(current, op.path.nodes, op.op, mode) match {
        case Right(updated) =>
          current = updated
        case Left(err) =>
          mode match {
            case JsonPatchMode.Strict  => error = Left(err)
            case JsonPatchMode.Lenient => ()
            case JsonPatchMode.Clobber => ()
          }
      }
      idx += 1
    }

    error.map(_ => current)
  }

  /**
   * Composes this patch with another. Applies this patch first, then `that`.
   */
  def ++(that: JsonPatch): JsonPatch = JsonPatch(ops ++ that.ops)

  /**
   * Returns true if this patch contains no operations.
   */
  def isEmpty: Boolean = ops.isEmpty

  /**
   * Converts this JSON patch to a [[DynamicPatch]].
   */
  def toDynamicPatch: DynamicPatch = JsonPatch.toDynamicPatch(this)

  override def toString: String =
    if (ops.isEmpty) "JsonPatch {}"
    else {
      val sb = new StringBuilder("JsonPatch {\n")
      ops.foreach(op => JsonPatch.renderOp(sb, op, "  "))
      sb.append("}")
      sb.toString
    }
}

object JsonPatch {

  /**
   * Empty patch — the identity element for `++` composition.
   */
  val empty: JsonPatch = JsonPatch(Vector.empty)

  /**
   * Creates a patch with a single operation at the root path.
   */
  def root(op: Op): JsonPatch = JsonPatch(Vector(JsonPatchOp(DynamicOptic.root, op)))

  /**
   * Creates a patch with a single operation at the given path.
   */
  def apply(path: DynamicOptic, op: Op): JsonPatch = JsonPatch(Vector(JsonPatchOp(path, op)))

  /**
   * Computes a patch that transforms `oldJson` into `newJson`.
   *
   * Law: `diff(old, new)(old, Strict) == Right(new)`
   */
  def diff(oldJson: Json, newJson: Json): JsonPatch =
    fromDynamicPatch(Differ.diff(toPatchDynamicValue(oldJson), toPatchDynamicValue(newJson))).getOrElse(
      root(Op.Set(newJson))
    )

  /**
   * Creates a JSON patch from a [[DynamicPatch]].
   *
   * Returns a [[JsonError]] if the DynamicPatch contains operations that are
   * not representable in JSON (e.g., non-string map keys or temporal deltas).
   */
  def fromDynamicPatch(patch: DynamicPatch): Either[JsonError, JsonPatch] = {
    val ops = Vector.newBuilder[JsonPatchOp]
    var idx = 0
    while (idx < patch.ops.length) {
      val op = patch.ops(idx)
      fromDynamicOperation(op.operation) match {
        case Right(converted) =>
          ops.addOne(JsonPatchOp(op.path, converted))
        case Left(err) =>
          return Left(err)
      }
      idx += 1
    }
    Right(JsonPatch(ops.result()))
  }

  private def toDynamicPatch(patch: JsonPatch): DynamicPatch =
    DynamicPatch(
      patch.ops.map(op => DynamicPatch.DynamicPatchOp(op.path, toDynamicOperation(op.op)))
    )

  private def renderOp(sb: StringBuilder, op: JsonPatchOp, indent: String): Unit = {
    val pathStr = op.path.toString
    op.op match {
      case Op.Set(value) =>
        sb.append(indent).append(pathStr).append(" = ").append(value).append("\n")

      case Op.PrimitiveDelta(primitiveOp) =>
        renderPrimitiveDelta(sb, pathStr, primitiveOp, indent)

      case Op.ArrayEdit(arrayOps) =>
        sb.append(indent).append(pathStr).append(":\n")
        arrayOps.foreach(ao => renderArrayOp(sb, ao, indent + "  "))

      case Op.ObjectEdit(objectOps) =>
        sb.append(indent).append(pathStr).append(":\n")
        objectOps.foreach(oo => renderObjectOp(sb, oo, indent + "  "))

      case Op.Nested(nestedPatch) =>
        sb.append(indent).append(pathStr).append(":\n")
        nestedPatch.ops.foreach(op => renderOp(sb, op, indent + "  "))
    }
  }

  private def renderPrimitiveDelta(sb: StringBuilder, pathStr: String, op: PrimitiveOp, indent: String): Unit =
    op match {
      case PrimitiveOp.NumberDelta(d) =>
        if (d >= 0) sb.append(indent).append(pathStr).append(" += ").append(d).append("\n")
        else sb.append(indent).append(pathStr).append(" -= ").append(-d).append("\n")
      case PrimitiveOp.StringEdit(ops) =>
        sb.append(indent).append(pathStr).append(":\n")
        ops.foreach {
          case StringOp.Insert(idx, text) =>
            sb.append(indent).append("  + [").append(idx).append(": ").append(escapeString(text)).append("]\n")
          case StringOp.Delete(idx, len) =>
            sb.append(indent).append("  - [").append(idx).append(", ").append(len).append("]\n")
          case StringOp.Append(text) =>
            sb.append(indent).append("  + ").append(escapeString(text)).append("\n")
          case StringOp.Modify(idx, len, text) =>
            sb.append(indent)
              .append("  ~ [")
              .append(idx)
              .append(", ")
              .append(len)
              .append(": ")
              .append(escapeString(text))
              .append("]\n")
        }
    }

  private def renderArrayOp(sb: StringBuilder, op: ArrayOp, indent: String): Unit =
    op match {
      case ArrayOp.Insert(index, values) =>
        values.zipWithIndex.foreach { case (v, i) =>
          sb.append(indent).append("+ [").append(index + i).append(": ").append(v).append("]\n")
        }
      case ArrayOp.Append(values) =>
        values.foreach { v =>
          sb.append(indent).append("+ ").append(v).append("\n")
        }
      case ArrayOp.Delete(index, count) =>
        if (count == 1) sb.append(indent).append("- [").append(index).append("]\n")
        else {
          val indices = (index until index + count).mkString(", ")
          sb.append(indent).append("- [").append(indices).append("]\n")
        }
      case ArrayOp.Modify(index, nestedOp) =>
        nestedOp match {
          case Op.Set(v) =>
            sb.append(indent).append("~ [").append(index).append(": ").append(v).append("]\n")
          case _ =>
            sb.append(indent).append("~ [").append(index).append("]:\n")
            renderOp(sb, JsonPatchOp(DynamicOptic.root, nestedOp), indent + "  ")
        }
    }

  private def renderObjectOp(sb: StringBuilder, op: ObjectOp, indent: String): Unit =
    op match {
      case ObjectOp.Add(k, v) =>
        sb.append(indent).append("+ {").append(k).append(": ").append(v).append("}\n")
      case ObjectOp.Remove(k) =>
        sb.append(indent).append("- {").append(k).append("}\n")
      case ObjectOp.Modify(k, patch) =>
        sb.append(indent).append("~ {").append(k).append("}:\n")
        patch.ops.foreach(op => renderOp(sb, op, indent + "  "))
    }

  private def escapeString(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '"'          => sb.append("\\\"")
      case '\\'         => sb.append("\\\\")
      case '\b'         => sb.append("\\b")
      case '\f'         => sb.append("\\f")
      case '\n'         => sb.append("\\n")
      case '\r'         => sb.append("\\r")
      case '\t'         => sb.append("\\t")
      case c if c < ' ' =>
        sb.append(f"\\u${c.toInt}%04x")
      case c =>
        sb.append(c)
    }
    sb.append("\"")
    sb.toString
  }

  private def applyOp(
    value: Json,
    path: IndexedSeq[DynamicOptic.Node],
    operation: Op,
    mode: JsonPatchMode
  ): Either[JsonError, Json] =
    if (path.isEmpty) {
      applyOperation(value, operation, mode, Nil)
    } else {
      navigateAndApply(value, path, 0, operation, mode, Nil)
    }

  private def navigateAndApply(
    value: Json,
    path: IndexedSeq[DynamicOptic.Node],
    pathIdx: Int,
    operation: Op,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Json] = {
    val node   = path(pathIdx)
    val isLast = pathIdx == path.length - 1

    node match {
      case DynamicOptic.Node.Field(name) =>
        value match {
          case obj: Json.Object =>
            val fieldIdx = obj.value.indexWhere(_._1 == name)
            if (fieldIdx < 0) {
              Left(buildError(trace, s"Missing field '$name'"))
            } else {
              val (fieldName, fieldValue) = obj.value(fieldIdx)
              val newTrace                = DynamicOptic.Node.Field(name) :: trace
              if (isLast) {
                applyOperation(fieldValue, operation, mode, newTrace).map { newFieldValue =>
                  new Json.Object(obj.value.updated(fieldIdx, (fieldName, newFieldValue)))
                }
              } else {
                navigateAndApply(fieldValue, path, pathIdx + 1, operation, mode, newTrace).map { newFieldValue =>
                  new Json.Object(obj.value.updated(fieldIdx, (fieldName, newFieldValue)))
                }
              }
            }

          case _ =>
            Left(buildError(trace, s"Expected Object but got ${value.jsonType}"))
        }

      case DynamicOptic.Node.AtIndex(index) =>
        value match {
          case arr: Json.Array =>
            if (index < 0 || index >= arr.value.length) {
              Left(buildError(trace, s"Index $index out of bounds for array of length ${arr.value.length}"))
            } else {
              val element  = arr.value(index)
              val newTrace = DynamicOptic.Node.AtIndex(index) :: trace
              if (isLast) {
                applyOperation(element, operation, mode, newTrace).map { newElement =>
                  new Json.Array(arr.value.updated(index, newElement))
                }
              } else {
                navigateAndApply(element, path, pathIdx + 1, operation, mode, newTrace).map { newElement =>
                  new Json.Array(arr.value.updated(index, newElement))
                }
              }
            }

          case _ =>
            Left(buildError(trace, s"Expected Array but got ${value.jsonType}"))
        }

      case DynamicOptic.Node.Elements =>
        value match {
          case arr: Json.Array =>
            val newTrace = DynamicOptic.Node.Elements :: trace
            if (arr.value.isEmpty) {
              mode match {
                case JsonPatchMode.Strict  => Left(buildError(newTrace, "Encountered an empty array"))
                case JsonPatchMode.Lenient => Right(value)
                case JsonPatchMode.Clobber => Right(value)
              }
            } else if (isLast) {
              applyToAllElements(arr.value, operation, mode, newTrace).map(elements => new Json.Array(elements))
            } else {
              navigateAllElements(arr.value, path, pathIdx + 1, operation, mode, newTrace).map(elements =>
                new Json.Array(elements)
              )
            }
          case _ =>
            Left(buildError(trace, s"Expected Array but got ${value.jsonType}"))
        }

      case other =>
        Left(buildError(trace, s"Unsupported path segment: $other"))
    }
  }

  private def applyOperation(
    value: Json,
    operation: Op,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Json] =
    operation match {
      case Op.Set(newValue) =>
        Right(newValue)

      case Op.PrimitiveDelta(op) =>
        applyPrimitiveDelta(value, op, trace)

      case Op.ArrayEdit(arrayOps) =>
        applyArrayEdit(value, arrayOps, mode, trace)

      case Op.ObjectEdit(objectOps) =>
        applyObjectEdit(value, objectOps, mode, trace)

      case Op.Nested(nestedPatch) =>
        nestedPatch.apply(value, mode)
    }

  private def applyPrimitiveDelta(
    value: Json,
    op: PrimitiveOp,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Json] =
    (value, op) match {
      case (num: Json.Number, PrimitiveOp.NumberDelta(delta)) =>
        val base = BigDecimal(num.value)
        Right(Json.Number((base + delta).toString))

      case (str: Json.String, PrimitiveOp.StringEdit(ops)) =>
        applyStringEdits(str.value, ops, trace).map(Json.String(_))

      case _ =>
        Left(buildError(trace, s"Type mismatch: cannot apply ${op.getClass.getSimpleName} to ${value.jsonType}"))
    }

  private def applyStringEdits(
    str: String,
    ops: Vector[StringOp],
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, String] = {
    var result                     = str
    var idx                        = 0
    var error: Option[JsonError] = None

    while (idx < ops.length && error.isEmpty) {
      ops(idx) match {
        case StringOp.Insert(index, text) =>
          if (index < 0 || index > result.length) {
            error = Some(
              buildError(
                trace,
                s"String insert index $index out of bounds for string of length ${result.length}"
              )
            )
          } else {
            result = result.substring(0, index) + text + result.substring(index)
          }

        case StringOp.Delete(index, length) =>
          if (index < 0 || index + length > result.length) {
            error = Some(
              buildError(
                trace,
                s"String delete range [$index, ${index + length}) out of bounds for string of length ${result.length}"
              )
            )
          } else {
            result = result.substring(0, index) + result.substring(index + length)
          }

        case StringOp.Append(text) =>
          result = result + text

        case StringOp.Modify(index, length, text) =>
          if (index < 0 || index + length > result.length) {
            error = Some(
              buildError(
                trace,
                s"String modify range [$index, ${index + length}) out of bounds for string of length ${result.length}"
              )
            )
          } else {
            result = result.substring(0, index) + text + result.substring(index + length)
          }
      }
      idx += 1
    }

    error.toLeft(result)
  }

  private def applyArrayEdit(
    value: Json,
    ops: Vector[ArrayOp],
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Json] =
    value match {
      case arr: Json.Array =>
        applyArrayOps(arr.value, ops, mode, trace).map(values => new Json.Array(values))
      case _ =>
        Left(buildError(trace, s"Expected Array but got ${value.jsonType}"))
    }

  private def applyArrayOps(
    elements: Chunk[Json],
    ops: Vector[ArrayOp],
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[Json]] = {
    var result                     = elements
    var idx                        = 0
    var error: Option[JsonError] = None

    while (idx < ops.length && error.isEmpty) {
      applyArrayOp(result, ops(idx), mode, trace) match {
        case Right(updated) => result = updated
        case Left(err) =>
          mode match {
            case JsonPatchMode.Strict  => error = Some(err)
            case JsonPatchMode.Lenient => ()
            case JsonPatchMode.Clobber => ()
          }
      }
      idx += 1
    }

    error.toLeft(result)
  }

  private def applyArrayOp(
    elements: Chunk[Json],
    op: ArrayOp,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[Json]] =
    op match {
      case ArrayOp.Append(values) =>
        Right(elements ++ Chunk.from(values))

      case ArrayOp.Insert(index, values) =>
        if (index < 0 || index > elements.length) {
          mode match {
            case JsonPatchMode.Strict =>
              Left(
                buildError(trace, s"Insert index $index out of bounds for array of length ${elements.length}")
              )
            case JsonPatchMode.Lenient =>
              Left(
                buildError(trace, s"Insert index $index out of bounds for array of length ${elements.length}")
              )
            case JsonPatchMode.Clobber =>
              val clampedIndex    = Math.max(0, Math.min(index, elements.length))
              val (before, after) = elements.splitAt(clampedIndex)
              Right(before ++ Chunk.from(values) ++ after)
          }
        } else {
          val (before, after) = elements.splitAt(index)
          Right(before ++ Chunk.from(values) ++ after)
        }

      case ArrayOp.Delete(index, count) =>
        if (index < 0 || index + count > elements.length) {
          mode match {
            case JsonPatchMode.Strict =>
              Left(
                buildError(
                  trace,
                  s"Delete range [$index, ${index + count}) out of bounds for array of length ${elements.length}"
                )
              )
            case JsonPatchMode.Lenient =>
              Left(
                buildError(
                  trace,
                  s"Delete range [$index, ${index + count}) out of bounds for array of length ${elements.length}"
                )
              )
            case JsonPatchMode.Clobber =>
              val clampedIndex = Math.max(0, Math.min(index, elements.length))
              val clampedEnd   = Math.max(0, Math.min(index + count, elements.length))
              Right(elements.take(clampedIndex) ++ elements.drop(clampedEnd))
          }
        } else {
          Right(elements.take(index) ++ elements.drop(index + count))
        }

      case ArrayOp.Modify(index, nestedOp) =>
        if (index < 0 || index >= elements.length) {
          Left(buildError(trace, s"Modify index $index out of bounds for array of length ${elements.length}"))
        } else {
          val element  = elements(index)
          val newTrace = DynamicOptic.Node.AtIndex(index) :: trace
          applyOperation(element, nestedOp, mode, newTrace).map { newElement =>
            elements.updated(index, newElement)
          }
        }
    }

  private def applyObjectEdit(
    value: Json,
    ops: Vector[ObjectOp],
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Json] =
    value match {
      case obj: Json.Object =>
        applyObjectOps(obj.value, ops, mode, trace).map(values => new Json.Object(values))
      case _ =>
        Left(buildError(trace, s"Expected Object but got ${value.jsonType}"))
    }

  private def applyObjectOps(
    entries: Chunk[(String, Json)],
    ops: Vector[ObjectOp],
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[(String, Json)]] = {
    var result                     = entries
    var idx                        = 0
    var error: Option[JsonError] = None

    while (idx < ops.length && error.isEmpty) {
      applyObjectOp(result, ops(idx), mode, trace) match {
        case Right(updated) => result = updated
        case Left(err) =>
          mode match {
            case JsonPatchMode.Strict  => error = Some(err)
            case JsonPatchMode.Lenient => ()
            case JsonPatchMode.Clobber => ()
          }
      }
      idx += 1
    }

    error.toLeft(result)
  }

  private def applyObjectOp(
    entries: Chunk[(String, Json)],
    op: ObjectOp,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[(String, Json)]] =
    op match {
      case ObjectOp.Add(key, value) =>
        val existingIdx = entries.indexWhere(_._1 == key)
        if (existingIdx >= 0) {
          mode match {
            case JsonPatchMode.Strict =>
              Left(buildError(trace, s"Key '$key' already exists in object"))
            case JsonPatchMode.Lenient =>
              Left(buildError(trace, s"Key '$key' already exists in object"))
            case JsonPatchMode.Clobber =>
              Right(entries.updated(existingIdx, (key, value)))
          }
        } else {
          Right(entries :+ (key, value))
        }

      case ObjectOp.Remove(key) =>
        val existingIdx = entries.indexWhere(_._1 == key)
        if (existingIdx < 0) {
          mode match {
            case JsonPatchMode.Strict =>
              Left(buildError(trace, s"Key '$key' not found in object"))
            case JsonPatchMode.Lenient =>
              Left(buildError(trace, s"Key '$key' not found in object"))
            case JsonPatchMode.Clobber =>
              Right(entries)
          }
        } else {
          Right(entries.take(existingIdx) ++ entries.drop(existingIdx + 1))
        }

      case ObjectOp.Modify(key, nestedPatch) =>
        val existingIdx = entries.indexWhere(_._1 == key)
        if (existingIdx < 0) {
          Left(buildError(trace, s"Key '$key' not found in object"))
        } else {
          val (k, v) = entries(existingIdx)
          nestedPatch.apply(v, mode).map { newValue =>
            entries.updated(existingIdx, (k, newValue))
          }
        }
    }

  private def applyToAllElements(
    elements: Chunk[Json],
    operation: Op,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[Json]] = {
    val builder = Chunk.newBuilder[Json]
    var idx     = 0
    while (idx < elements.length) {
      val elementTrace = DynamicOptic.Node.AtIndex(idx) :: trace
      applyOperation(elements(idx), operation, mode, elementTrace) match {
        case Right(updated) =>
          builder += updated
        case Left(err) =>
          mode match {
            case JsonPatchMode.Strict  => return Left(err)
            case JsonPatchMode.Lenient => builder += elements(idx)
            case JsonPatchMode.Clobber => builder += elements(idx)
          }
      }
      idx += 1
    }
    Right(builder.result())
  }

  private def navigateAllElements(
    elements: Chunk[Json],
    path: IndexedSeq[DynamicOptic.Node],
    pathIdx: Int,
    operation: Op,
    mode: JsonPatchMode,
    trace: List[DynamicOptic.Node]
  ): Either[JsonError, Chunk[Json]] = {
    val builder = Chunk.newBuilder[Json]
    var idx     = 0
    while (idx < elements.length) {
      val elementTrace = DynamicOptic.Node.AtIndex(idx) :: trace
      navigateAndApply(elements(idx), path, pathIdx, operation, mode, elementTrace) match {
        case Right(updated) =>
          builder += updated
        case Left(err) =>
          mode match {
            case JsonPatchMode.Strict  => return Left(err)
            case JsonPatchMode.Lenient => builder += elements(idx)
            case JsonPatchMode.Clobber => builder += elements(idx)
          }
      }
      idx += 1
    }
    Right(builder.result())
  }

  private def buildError(trace: List[DynamicOptic.Node], message: String): JsonError = {
    val base = JsonError(message)
    trace.reverse.foldLeft(base) { (err, node) =>
      node match {
        case DynamicOptic.Node.Field(name)    => err.atField(name)
        case DynamicOptic.Node.AtIndex(index) => err.atIndex(index)
        case _                                => err
      }
    }
  }

  private def toDynamicOperation(op: Op): DynamicPatch.Operation =
    op match {
      case Op.Set(value) =>
        DynamicPatch.Operation.Set(toPatchDynamicValue(value))
      case Op.PrimitiveDelta(primitiveOp) =>
        DynamicPatch.Operation.PrimitiveDelta(toDynamicPrimitiveOp(primitiveOp))
      case Op.ArrayEdit(ops) =>
        DynamicPatch.Operation.SequenceEdit(ops.map(toDynamicArrayOp))
      case Op.ObjectEdit(ops) =>
        DynamicPatch.Operation.MapEdit(ops.map(toDynamicObjectOp))
      case Op.Nested(patch) =>
        DynamicPatch.Operation.Patch(toDynamicPatch(patch))
    }

  private def toDynamicPrimitiveOp(op: PrimitiveOp): DynamicPatch.PrimitiveOp =
    op match {
      case PrimitiveOp.NumberDelta(delta) =>
        DynamicPatch.PrimitiveOp.BigDecimalDelta(delta)
      case PrimitiveOp.StringEdit(ops) =>
        DynamicPatch.PrimitiveOp.StringEdit(ops.map(toDynamicStringOp))
    }

  private def toDynamicStringOp(op: StringOp): DynamicPatch.StringOp =
    op match {
      case StringOp.Insert(index, text) =>
        DynamicPatch.StringOp.Insert(index, text)
      case StringOp.Delete(index, length) =>
        DynamicPatch.StringOp.Delete(index, length)
      case StringOp.Append(text) =>
        DynamicPatch.StringOp.Append(text)
      case StringOp.Modify(index, length, text) =>
        DynamicPatch.StringOp.Modify(index, length, text)
    }

  private def toDynamicArrayOp(op: ArrayOp): DynamicPatch.SeqOp =
    op match {
      case ArrayOp.Insert(index, values) =>
        DynamicPatch.SeqOp.Insert(index, Chunk.from(values.map(toPatchDynamicValue)))
      case ArrayOp.Append(values) =>
        DynamicPatch.SeqOp.Append(Chunk.from(values.map(toPatchDynamicValue)))
      case ArrayOp.Delete(index, count) =>
        DynamicPatch.SeqOp.Delete(index, count)
      case ArrayOp.Modify(index, nestedOp) =>
        DynamicPatch.SeqOp.Modify(index, toDynamicOperation(nestedOp))
    }

  private def toDynamicObjectOp(op: ObjectOp): DynamicPatch.MapOp =
    op match {
      case ObjectOp.Add(key, value) =>
        DynamicPatch.MapOp.Add(toStringKey(key), toPatchDynamicValue(value))
      case ObjectOp.Remove(key) =>
        DynamicPatch.MapOp.Remove(toStringKey(key))
      case ObjectOp.Modify(key, patch) =>
        DynamicPatch.MapOp.Modify(toStringKey(key), toDynamicPatch(patch))
    }

  private def fromDynamicOperation(op: DynamicPatch.Operation): Either[JsonError, Op] =
    op match {
      case DynamicPatch.Operation.Set(value) =>
        Right(Op.Set(Json.fromDynamicValue(value)))
      case DynamicPatch.Operation.PrimitiveDelta(primitiveOp) =>
        fromDynamicPrimitiveOp(primitiveOp).map(Op.PrimitiveDelta(_))
      case DynamicPatch.Operation.SequenceEdit(ops) =>
        fromDynamicArrayOps(ops).map(Op.ArrayEdit(_))
      case DynamicPatch.Operation.MapEdit(ops) =>
        fromDynamicObjectOps(ops).map(Op.ObjectEdit(_))
      case DynamicPatch.Operation.Patch(patch) =>
        fromDynamicPatch(patch).map(Op.Nested(_))
    }

  private def fromDynamicPrimitiveOp(op: DynamicPatch.PrimitiveOp): Either[JsonError, PrimitiveOp] =
    op match {
      case DynamicPatch.PrimitiveOp.IntDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta)))
      case DynamicPatch.PrimitiveOp.LongDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta)))
      case DynamicPatch.PrimitiveOp.DoubleDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta)))
      case DynamicPatch.PrimitiveOp.FloatDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta.toDouble)))
      case DynamicPatch.PrimitiveOp.ShortDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta.toInt)))
      case DynamicPatch.PrimitiveOp.ByteDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta.toInt)))
      case DynamicPatch.PrimitiveOp.BigIntDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(BigDecimal(delta)))
      case DynamicPatch.PrimitiveOp.BigDecimalDelta(delta) =>
        Right(PrimitiveOp.NumberDelta(delta))
      case DynamicPatch.PrimitiveOp.StringEdit(ops) =>
        Right(PrimitiveOp.StringEdit(ops.map(fromDynamicStringOp)))
      case other =>
        Left(JsonError(s"Unsupported primitive delta for JsonPatch: ${other.getClass.getSimpleName}"))
    }

  private def fromDynamicStringOp(op: DynamicPatch.StringOp): StringOp =
    op match {
      case DynamicPatch.StringOp.Insert(index, text) =>
        StringOp.Insert(index, text)
      case DynamicPatch.StringOp.Delete(index, length) =>
        StringOp.Delete(index, length)
      case DynamicPatch.StringOp.Append(text) =>
        StringOp.Append(text)
      case DynamicPatch.StringOp.Modify(index, length, text) =>
        StringOp.Modify(index, length, text)
    }

  private def fromDynamicArrayOps(ops: Vector[DynamicPatch.SeqOp]): Either[JsonError, Vector[ArrayOp]] = {
    val builder = Vector.newBuilder[ArrayOp]
    var idx     = 0
    while (idx < ops.length) {
      val op = ops(idx)
      val converted = op match {
        case DynamicPatch.SeqOp.Insert(index, values) =>
          Right(ArrayOp.Insert(index, values.toVector.map(Json.fromDynamicValue)))
        case DynamicPatch.SeqOp.Append(values) =>
          Right(ArrayOp.Append(values.toVector.map(Json.fromDynamicValue)))
        case DynamicPatch.SeqOp.Delete(index, count) =>
          Right(ArrayOp.Delete(index, count))
        case DynamicPatch.SeqOp.Modify(index, nestedOp) =>
          fromDynamicOperation(nestedOp).map(ArrayOp.Modify(index, _))
      }
      converted match {
        case Right(value) =>
          builder += value
        case Left(err) =>
          return Left(err)
      }
      idx += 1
    }
    Right(builder.result())
  }

  private def fromDynamicObjectOps(ops: Vector[DynamicPatch.MapOp]): Either[JsonError, Vector[ObjectOp]] = {
    val builder = Vector.newBuilder[ObjectOp]
    var idx     = 0
    while (idx < ops.length) {
      val op = ops(idx)
      val converted = op match {
        case DynamicPatch.MapOp.Add(key, value) =>
          stringKey(key).map(ObjectOp.Add(_, Json.fromDynamicValue(value)))
        case DynamicPatch.MapOp.Remove(key) =>
          stringKey(key).map(ObjectOp.Remove(_))
        case DynamicPatch.MapOp.Modify(key, patch) =>
          stringKey(key).flatMap(k => fromDynamicPatch(patch).map(ObjectOp.Modify(k, _)))
      }
      converted match {
        case Right(value) =>
          builder += value
        case Left(err) =>
          return Left(err)
      }
      idx += 1
    }
    Right(builder.result())
  }

  private def stringKey(key: DynamicValue): Either[JsonError, String] =
    key match {
      case DynamicValue.Primitive(PrimitiveValue.String(value)) => Right(value)
      case other =>
        Left(JsonError(s"JsonPatch only supports string object keys, found: ${other.getClass.getSimpleName}"))
    }

  private def toStringKey(key: String): DynamicValue =
    DynamicValue.Primitive(PrimitiveValue.String(key))

  private def toPatchDynamicValue(json: Json): DynamicValue = json match {
    case Json.Null          => DynamicValue.Null
    case str: Json.String   => DynamicValue.Primitive(PrimitiveValue.String(str.value))
    case bool: Json.Boolean => DynamicValue.Primitive(PrimitiveValue.Boolean(bool.value))
    case num: Json.Number   => DynamicValue.Primitive(PrimitiveValue.BigDecimal(BigDecimal(num.value)))
    case arr: Json.Array    => DynamicValue.Sequence(arr.value.map(toPatchDynamicValue))
    case obj: Json.Object =>
      val entries = obj.value.map { case (k, v) =>
        (DynamicValue.Primitive(PrimitiveValue.String(k)), toPatchDynamicValue(v))
      }
      DynamicValue.Map(entries)
  }

  // ===========================================================================
  // JsonPatchOp — a single operation at a path
  // ===========================================================================

  /**
   * A single patch operation: a path and what to do there.
   */
  final case class JsonPatchOp(path: DynamicOptic, op: Op)

  // ===========================================================================
  // Operation Types
  // ===========================================================================

  sealed trait Op

  object Op {

    /**
     * Set a value directly (clobber semantics). Replaces the target value entirely.
     */
    final case class Set(value: Json) extends Op

    /**
     * Apply a primitive delta operation (number delta or string edit).
     */
    final case class PrimitiveDelta(op: PrimitiveOp) extends Op

    /**
     * Apply array edit operations (insert, append, delete, modify).
     */
    final case class ArrayEdit(ops: Vector[ArrayOp]) extends Op

    /**
     * Apply object edit operations (add, remove, modify).
     */
    final case class ObjectEdit(ops: Vector[ObjectOp]) extends Op

    /**
     * Apply a nested patch, grouping multiple operations with a common prefix.
     */
    final case class Nested(patch: JsonPatch) extends Op
  }

  // ===========================================================================
  // Primitive Operations
  // ===========================================================================

  sealed trait PrimitiveOp

  object PrimitiveOp {

    /**
     * Number delta for JSON numbers.
     */
    final case class NumberDelta(delta: BigDecimal) extends PrimitiveOp

    /**
     * String edit operations for JSON strings.
     */
    final case class StringEdit(ops: Vector[StringOp]) extends PrimitiveOp
  }

  sealed trait StringOp

  object StringOp {

    /** Inserts text at the given index. */
    final case class Insert(index: Int, text: String) extends StringOp

    /** Deletes `length` characters starting at `index`. */
    final case class Delete(index: Int, length: Int) extends StringOp

    /** Appends text to the end of the string. */
    final case class Append(text: String) extends StringOp

    /** Replaces the substring at `[index, index + length)` with `text`. */
    final case class Modify(index: Int, length: Int, text: String) extends StringOp
  }

  sealed trait ArrayOp

  object ArrayOp {

    /** Inserts values at the given index. */
    final case class Insert(index: Int, values: Vector[Json]) extends ArrayOp

    /** Appends values to the end of the array. */
    final case class Append(values: Vector[Json]) extends ArrayOp

    /** Deletes `count` elements starting at `index`. */
    final case class Delete(index: Int, count: Int) extends ArrayOp

    /** Modifies the element at `index` using a nested operation. */
    final case class Modify(index: Int, op: Op) extends ArrayOp
  }

  sealed trait ObjectOp

  object ObjectOp {

    /** Adds a key/value pair to an object. */
    final case class Add(key: String, value: Json) extends ObjectOp

    /** Removes a key from an object. */
    final case class Remove(key: String) extends ObjectOp

    /** Modifies the value at a key using a nested patch. */
    final case class Modify(key: String, patch: JsonPatch) extends ObjectOp
  }
}
