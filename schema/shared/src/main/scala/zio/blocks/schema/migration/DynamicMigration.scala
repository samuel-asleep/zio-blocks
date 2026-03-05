package zio.blocks.schema.migration

import zio.blocks.chunk.Chunk
import zio.blocks.schema._
import zio.blocks.schema.binding._
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.typeid.TypeId

/**
 * An untyped, serializable migration that operates on [[DynamicValue]] instances by
 * applying a sequence of [[MigrationAction]] steps.
 *
 * {{{
 * // Rename field "name" to "fullName" and add a new "active" field
 * val migration = DynamicMigration(Vector(
 *   MigrationAction.Rename(DynamicOptic.root.field("name"), "fullName"),
 *   MigrationAction.AddField(
 *     DynamicOptic.root.field("active"),
 *     DynamicValue.Primitive(PrimitiveValue.Boolean(true))
 *   )
 * ))
 *
 * val source = DynamicValue.Record(Chunk("name" -> DynamicValue.Primitive(PrimitiveValue.String("Alice"))))
 * migration(source) match {
 *   case Right(result) => // DynamicValue.Record with "fullName" and "active" fields
 *   case Left(err)     => // SchemaError describing what failed
 * }
 *
 * // Compose two migrations
 * val combined = migration ++ migration.reverse
 * }}}
 *
 * @param actions the ordered list of actions to apply
 */
final case class DynamicMigration(actions: Vector[MigrationAction]) {

  /**
   * Applies this migration to a [[DynamicValue]], returning either the migrated value
   * or a [[SchemaError]] if any action fails.
   */
  def apply(value: DynamicValue): Either[SchemaError, DynamicValue] = {
    var current = value
    val len     = actions.length
    var i       = 0
    while (i < len) {
      DynamicMigration.applyAction(current, actions(i)) match {
        case Right(updated) => current = updated
        case l              => return l
      }
      i += 1
    }
    new Right(current)
  }

  /** Composes two migrations sequentially: applies this migration first, then `that`. */
  def ++(that: DynamicMigration): DynamicMigration = DynamicMigration(this.actions ++ that.actions)

  /** Returns a migration that undoes this migration by reversing each action in reverse order. */
  def reverse: DynamicMigration = DynamicMigration(actions.reverse.map(_.reverse))

  /** Returns true if this migration has no actions. */
  def isEmpty: Boolean = actions.isEmpty
}

object DynamicMigration {

  /** An identity migration that leaves any value unchanged. */
  val identity: DynamicMigration = DynamicMigration(Vector.empty)

  private[migration] def applyAction(
    current: DynamicValue,
    action: MigrationAction
  ): Either[SchemaError, DynamicValue] = action match {

    case MigrationAction.AddField(at, default) =>
      current.insertOrFail(at, default)

    case MigrationAction.DropField(at, _) =>
      current.deleteOrFail(at)

    case MigrationAction.Rename(at, to) =>
      val nodes = at.nodes
      if (nodes.isEmpty) Left(SchemaError.message("Rename: path must not be root"))
      else
        nodes.last match {
          case _: DynamicOptic.Node.Field =>
            val parentPath = new DynamicOptic(nodes.dropRight(1))
            val newAt      = parentPath.field(to)
            current.get(at).one.flatMap { value =>
              current.deleteOrFail(at).flatMap { withoutOld =>
                withoutOld.insertOrFail(newAt, value)
              }
            }
          case node => Left(SchemaError.message(s"Rename expects Field node, got: $node"))
        }

    case MigrationAction.TransformValue(at, migration) =>
      current.get(at).one.flatMap { value =>
        migration(value).flatMap { newValue =>
          if (at.nodes.isEmpty) new Right(newValue)
          else current.setOrFail(at, newValue)
        }
      }

    case MigrationAction.Mandate(at, _) =>
      current.get(at).one.flatMap {
        case DynamicValue.Variant(caseName, caseValue) =>
          caseName match {
            case "Some" => current.setOrFail(at, caseValue)
            case "None" => Left(SchemaError.message("Mandate: field is None, cannot make mandatory", at))
            case other  => Left(SchemaError.message(s"Mandate expects Some/None Variant, got: $other", at))
          }
        case other => Left(SchemaError.message(s"Mandate expects Variant, got: ${other.valueType}", at))
      }

    case MigrationAction.Optionalize(at) =>
      current.get(at).one.flatMap { value =>
        current.setOrFail(at, DynamicValue.Variant("Some", value))
      }

    case MigrationAction.ChangeType(at, converter) =>
      current.get(at).one.flatMap {
        case DynamicValue.Primitive(pv) =>
          converter(pv).flatMap { newPv =>
            current.setOrFail(at, DynamicValue.Primitive(newPv))
          }
        case other => Left(SchemaError.message(s"ChangeType expects Primitive, got: ${other.valueType}", at))
      }

    case MigrationAction.RenameCase(at, to) =>
      val nodes = at.nodes
      if (nodes.isEmpty) Left(SchemaError.message("RenameCase: path must not be root"))
      else
        nodes.last match {
          case DynamicOptic.Node.Case(originalName) =>
            val parentPath = new DynamicOptic(nodes.dropRight(1))
            current.get(parentPath).one.flatMap {
              case DynamicValue.Variant(caseName, caseValue) if caseName == originalName =>
                current.setOrFail(parentPath, DynamicValue.Variant(to, caseValue))
              case DynamicValue.Variant(caseName, _) =>
                Left(SchemaError.message(s"RenameCase: expected case '$originalName', got '$caseName'", at))
              case other =>
                Left(SchemaError.message(s"RenameCase expects Variant, got: ${other.valueType}", at))
            }
          case node => Left(SchemaError.message(s"RenameCase expects Case node, got: $node"))
        }

    case MigrationAction.TransformCase(at, caseActions) =>
      val nodes = at.nodes
      if (nodes.isEmpty) Left(SchemaError.message("TransformCase: path must not be root"))
      else
        nodes.last match {
          case DynamicOptic.Node.Case(caseName) =>
            val parentPath    = new DynamicOptic(nodes.dropRight(1))
            val subMigration  = DynamicMigration(caseActions)
            current.get(parentPath).one.flatMap {
              case DynamicValue.Variant(cn, caseValue) if cn == caseName =>
                subMigration(caseValue).flatMap { newCaseValue =>
                  current.setOrFail(parentPath, DynamicValue.Variant(caseName, newCaseValue))
                }
              case DynamicValue.Variant(cn, _) =>
                Left(SchemaError.message(s"TransformCase: expected case '$caseName', got '$cn'", at))
              case other =>
                Left(SchemaError.message(s"TransformCase expects Variant, got: ${other.valueType}", at))
            }
          case node => Left(SchemaError.message(s"TransformCase expects Case node, got: $node"))
        }

    case MigrationAction.TransformElements(at, transform) =>
      current.get(at).one.flatMap {
        case DynamicValue.Sequence(elements) =>
          val builder = Chunk.newBuilder[DynamicValue]
          var err: SchemaError = null
          val iter = elements.iterator
          while (err == null && iter.hasNext) {
            transform(iter.next()) match {
              case Right(v) => builder += v
              case Left(e)  => err = e
            }
          }
          if (err != null) Left(err)
          else current.setOrFail(at, DynamicValue.Sequence(builder.result()))
        case other =>
          Left(SchemaError.message(s"TransformElements expects Sequence, got: ${other.valueType}", at))
      }

    case MigrationAction.TransformKeys(at, transform) =>
      current.get(at).one.flatMap {
        case DynamicValue.Map(entries) =>
          val builder = Chunk.newBuilder[(DynamicValue, DynamicValue)]
          var err: SchemaError = null
          val iter = entries.iterator
          while (err == null && iter.hasNext) {
            val (k, v) = iter.next()
            transform(k) match {
              case Right(newK) => builder += ((newK, v))
              case Left(e)     => err = e
            }
          }
          if (err != null) Left(err)
          else current.setOrFail(at, DynamicValue.Map(builder.result()))
        case other =>
          Left(SchemaError.message(s"TransformKeys expects Map, got: ${other.valueType}", at))
      }

    case MigrationAction.TransformValues(at, transform) =>
      current.get(at).one.flatMap {
        case DynamicValue.Map(entries) =>
          val builder = Chunk.newBuilder[(DynamicValue, DynamicValue)]
          var err: SchemaError = null
          val iter = entries.iterator
          while (err == null && iter.hasNext) {
            val (k, v) = iter.next()
            transform(v) match {
              case Right(newV) => builder += ((k, newV))
              case Left(e)     => err = e
            }
          }
          if (err != null) Left(err)
          else current.setOrFail(at, DynamicValue.Map(builder.result()))
        case other =>
          Left(SchemaError.message(s"TransformValues expects Map, got: ${other.valueType}", at))
      }
  }

  implicit lazy val schema: Schema[DynamicMigration] = new Schema(
    reflect = new Reflect.Record[Binding, DynamicMigration](
      fields = Chunk.single(
        Reflect.Deferred(() => Schema.vector(MigrationAction.schema).reflect).asTerm("actions")
      ),
      typeId = TypeId.of[DynamicMigration],
      recordBinding = new Binding.Record(
        constructor = new Constructor[DynamicMigration] {
          def usedRegisters: RegisterOffset                     = RegisterOffset(objects = 1)
          def construct(in: Registers, offset: RegisterOffset): DynamicMigration =
            new DynamicMigration(in.getObject(offset).asInstanceOf[Vector[MigrationAction]])
        },
        deconstructor = new Deconstructor[DynamicMigration] {
          def usedRegisters: RegisterOffset                                          = RegisterOffset(objects = 1)
          def deconstruct(out: Registers, offset: RegisterOffset, in: DynamicMigration): Unit =
            out.setObject(offset, in.actions)
        }
      ),
      modifiers = Chunk.empty
    )
  )
}
