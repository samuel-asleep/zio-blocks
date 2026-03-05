package zio.blocks.schema.migration

import zio.blocks.chunk.Chunk
import zio.blocks.schema._
import zio.blocks.schema.binding._
import zio.blocks.schema.binding.RegisterOffset.RegisterOffset
import zio.blocks.typeid.TypeId

/**
 * A converter between two primitive types. Used in
 * [[MigrationAction.ChangeType]] to change the primitive type of a value at a
 * given path.
 *
 * @example
 *   {{{
 * val result = TypeConverter.IntToString(PrimitiveValue.Int(42))
 * // Right(PrimitiveValue.String("42"))
 *
 * val back = TypeConverter.IntToString.reverse(PrimitiveValue.String("42"))
 * // Right(PrimitiveValue.Int(42))
 *   }}}
 */
sealed trait TypeConverter {

  /**
   * Applies this converter to a [[PrimitiveValue]], returning the result or an
   * error.
   */
  def apply(value: PrimitiveValue): Either[SchemaError, PrimitiveValue]

  /** Returns the structural inverse of this converter. */
  def reverse: TypeConverter
}

object TypeConverter {

  /** Converts an `Int` to its decimal `String` representation. */
  case object IntToString extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Int(n) => Right(PrimitiveValue.String(n.toString))
      case _                     => Left(SchemaError.message("IntToString expects Int"))
    }
    def reverse: TypeConverter = StringToInt
  }

  /**
   * Parses a `String` as an `Int`. Fails if the string is not a valid integer.
   */
  case object StringToInt extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.String(s) =>
        s.toIntOption match {
          case Some(n) => Right(PrimitiveValue.Int(n))
          case None    => Left(SchemaError.message(s"StringToInt: cannot parse '$s' as Int"))
        }
      case _ => Left(SchemaError.message("StringToInt expects String"))
    }
    def reverse: TypeConverter = IntToString
  }

  /** Converts a `Long` to its decimal `String` representation. */
  case object LongToString extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Long(n) => Right(PrimitiveValue.String(n.toString))
      case _                      => Left(SchemaError.message("LongToString expects Long"))
    }
    def reverse: TypeConverter = StringToLong
  }

  /**
   * Parses a `String` as a `Long`. Fails if the string is not a valid long
   * integer.
   */
  case object StringToLong extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.String(s) =>
        s.toLongOption match {
          case Some(n) => Right(PrimitiveValue.Long(n))
          case None    => Left(SchemaError.message(s"StringToLong: cannot parse '$s' as Long"))
        }
      case _ => Left(SchemaError.message("StringToLong expects String"))
    }
    def reverse: TypeConverter = LongToString
  }

  /** Widens an `Int` to a `Long`. Always succeeds. */
  case object IntToLong extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Int(n) => Right(PrimitiveValue.Long(n.toLong))
      case _                     => Left(SchemaError.message("IntToLong expects Int"))
    }
    def reverse: TypeConverter = LongToInt
  }

  /** Narrows a `Long` to an `Int`. Truncates on overflow. */
  case object LongToInt extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Long(n) => Right(PrimitiveValue.Int(n.toInt))
      case _                      => Left(SchemaError.message("LongToInt expects Long"))
    }
    def reverse: TypeConverter = IntToLong
  }

  /** Converts a `Double` to its `String` representation. */
  case object DoubleToString extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Double(n) => Right(PrimitiveValue.String(n.toString))
      case _                        => Left(SchemaError.message("DoubleToString expects Double"))
    }
    def reverse: TypeConverter = StringToDouble
  }

  /**
   * Parses a `String` as a `Double`. Fails if the string is not a valid double.
   */
  case object StringToDouble extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.String(s) =>
        s.toDoubleOption match {
          case Some(n) => Right(PrimitiveValue.Double(n))
          case None    => Left(SchemaError.message(s"StringToDouble: cannot parse '$s' as Double"))
        }
      case _ => Left(SchemaError.message("StringToDouble expects String"))
    }
    def reverse: TypeConverter = DoubleToString
  }

  /** Widens a `Float` to a `Double`. Always succeeds. */
  case object FloatToDouble extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Float(n) => Right(PrimitiveValue.Double(n.toDouble))
      case _                       => Left(SchemaError.message("FloatToDouble expects Float"))
    }
    def reverse: TypeConverter = DoubleToFloat
  }

  /** Narrows a `Double` to a `Float`. May lose precision. */
  case object DoubleToFloat extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Double(n) => Right(PrimitiveValue.Float(n.toFloat))
      case _                        => Left(SchemaError.message("DoubleToFloat expects Double"))
    }
    def reverse: TypeConverter = FloatToDouble
  }

  /**
   * Converts a `Boolean` to its `String` representation (`"true"` or
   * `"false"`).
   */
  case object BoolToString extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.Boolean(b) => Right(PrimitiveValue.String(b.toString))
      case _                         => Left(SchemaError.message("BoolToString expects Boolean"))
    }
    def reverse: TypeConverter = StringToBool
  }

  /**
   * Parses a `String` as a `Boolean`. Accepts `"true"` / `"false"`
   * (case-insensitive). Fails otherwise.
   */
  case object StringToBool extends TypeConverter {
    def apply(v: PrimitiveValue): Either[SchemaError, PrimitiveValue] = v match {
      case PrimitiveValue.String(s) =>
        s.toLowerCase match {
          case "true"  => Right(PrimitiveValue.Boolean(true))
          case "false" => Right(PrimitiveValue.Boolean(false))
          case _       => Left(SchemaError.message(s"StringToBool: cannot parse '$s' as Boolean"))
        }
      case _ => Left(SchemaError.message("StringToBool expects String"))
    }
    def reverse: TypeConverter = BoolToString
  }

  implicit lazy val intToStringSchema: Schema[TypeConverter.IntToString.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.IntToString.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.IntToString.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.IntToString.type] {
          def usedRegisters: RegisterOffset                                                    = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.IntToString.type =
            TypeConverter.IntToString
        },
        deconstructor = new Deconstructor[TypeConverter.IntToString.type] {
          def usedRegisters: RegisterOffset                                                                 = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.IntToString.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val stringToIntSchema: Schema[TypeConverter.StringToInt.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.StringToInt.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.StringToInt.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.StringToInt.type] {
          def usedRegisters: RegisterOffset                                                    = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.StringToInt.type =
            TypeConverter.StringToInt
        },
        deconstructor = new Deconstructor[TypeConverter.StringToInt.type] {
          def usedRegisters: RegisterOffset                                                                 = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.StringToInt.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val longToStringSchema: Schema[TypeConverter.LongToString.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.LongToString.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.LongToString.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.LongToString.type] {
          def usedRegisters: RegisterOffset                                                     = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.LongToString.type =
            TypeConverter.LongToString
        },
        deconstructor = new Deconstructor[TypeConverter.LongToString.type] {
          def usedRegisters: RegisterOffset                                                                  = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.LongToString.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val stringToLongSchema: Schema[TypeConverter.StringToLong.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.StringToLong.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.StringToLong.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.StringToLong.type] {
          def usedRegisters: RegisterOffset                                                     = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.StringToLong.type =
            TypeConverter.StringToLong
        },
        deconstructor = new Deconstructor[TypeConverter.StringToLong.type] {
          def usedRegisters: RegisterOffset                                                                  = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.StringToLong.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val intToLongSchema: Schema[TypeConverter.IntToLong.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.IntToLong.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.IntToLong.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.IntToLong.type] {
          def usedRegisters: RegisterOffset                                                  = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.IntToLong.type = TypeConverter.IntToLong
        },
        deconstructor = new Deconstructor[TypeConverter.IntToLong.type] {
          def usedRegisters: RegisterOffset                                                               = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.IntToLong.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val longToIntSchema: Schema[TypeConverter.LongToInt.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.LongToInt.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.LongToInt.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.LongToInt.type] {
          def usedRegisters: RegisterOffset                                                  = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.LongToInt.type = TypeConverter.LongToInt
        },
        deconstructor = new Deconstructor[TypeConverter.LongToInt.type] {
          def usedRegisters: RegisterOffset                                                               = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.LongToInt.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val doubleToStringSchema: Schema[TypeConverter.DoubleToString.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.DoubleToString.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.DoubleToString.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.DoubleToString.type] {
          def usedRegisters: RegisterOffset                                                       = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.DoubleToString.type =
            TypeConverter.DoubleToString
        },
        deconstructor = new Deconstructor[TypeConverter.DoubleToString.type] {
          def usedRegisters: RegisterOffset                                                                    = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.DoubleToString.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val stringToDoubleSchema: Schema[TypeConverter.StringToDouble.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.StringToDouble.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.StringToDouble.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.StringToDouble.type] {
          def usedRegisters: RegisterOffset                                                       = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.StringToDouble.type =
            TypeConverter.StringToDouble
        },
        deconstructor = new Deconstructor[TypeConverter.StringToDouble.type] {
          def usedRegisters: RegisterOffset                                                                    = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.StringToDouble.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val floatToDoubleSchema: Schema[TypeConverter.FloatToDouble.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.FloatToDouble.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.FloatToDouble.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.FloatToDouble.type] {
          def usedRegisters: RegisterOffset                                                      = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.FloatToDouble.type =
            TypeConverter.FloatToDouble
        },
        deconstructor = new Deconstructor[TypeConverter.FloatToDouble.type] {
          def usedRegisters: RegisterOffset                                                                   = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.FloatToDouble.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val doubleToFloatSchema: Schema[TypeConverter.DoubleToFloat.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.DoubleToFloat.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.DoubleToFloat.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.DoubleToFloat.type] {
          def usedRegisters: RegisterOffset                                                      = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.DoubleToFloat.type =
            TypeConverter.DoubleToFloat
        },
        deconstructor = new Deconstructor[TypeConverter.DoubleToFloat.type] {
          def usedRegisters: RegisterOffset                                                                   = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.DoubleToFloat.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val boolToStringSchema: Schema[TypeConverter.BoolToString.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.BoolToString.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.BoolToString.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.BoolToString.type] {
          def usedRegisters: RegisterOffset                                                     = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.BoolToString.type =
            TypeConverter.BoolToString
        },
        deconstructor = new Deconstructor[TypeConverter.BoolToString.type] {
          def usedRegisters: RegisterOffset                                                                  = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.BoolToString.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val stringToBoolSchema: Schema[TypeConverter.StringToBool.type] = new Schema(
    reflect = new Reflect.Record[Binding, TypeConverter.StringToBool.type](
      fields = Chunk.empty,
      typeId = TypeId.of[TypeConverter.StringToBool.type],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TypeConverter.StringToBool.type] {
          def usedRegisters: RegisterOffset                                                     = RegisterOffset()
          def construct(in: Registers, offset: RegisterOffset): TypeConverter.StringToBool.type =
            TypeConverter.StringToBool
        },
        deconstructor = new Deconstructor[TypeConverter.StringToBool.type] {
          def usedRegisters: RegisterOffset                                                                  = RegisterOffset()
          def deconstruct(out: Registers, offset: RegisterOffset, in: TypeConverter.StringToBool.type): Unit = ()
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val schema: Schema[TypeConverter] = new Schema(
    reflect = new Reflect.Variant[Binding, TypeConverter](
      cases = Chunk(
        intToStringSchema.reflect.asTerm("IntToString"),
        stringToIntSchema.reflect.asTerm("StringToInt"),
        longToStringSchema.reflect.asTerm("LongToString"),
        stringToLongSchema.reflect.asTerm("StringToLong"),
        intToLongSchema.reflect.asTerm("IntToLong"),
        longToIntSchema.reflect.asTerm("LongToInt"),
        doubleToStringSchema.reflect.asTerm("DoubleToString"),
        stringToDoubleSchema.reflect.asTerm("StringToDouble"),
        floatToDoubleSchema.reflect.asTerm("FloatToDouble"),
        doubleToFloatSchema.reflect.asTerm("DoubleToFloat"),
        boolToStringSchema.reflect.asTerm("BoolToString"),
        stringToBoolSchema.reflect.asTerm("StringToBool")
      ),
      typeId = TypeId.of[TypeConverter],
      variantBinding = new Binding.Variant(
        discriminator = new Discriminator[TypeConverter] {
          def discriminate(a: TypeConverter): Int = a match {
            case TypeConverter.IntToString    => 0
            case TypeConverter.StringToInt    => 1
            case TypeConverter.LongToString   => 2
            case TypeConverter.StringToLong   => 3
            case TypeConverter.IntToLong      => 4
            case TypeConverter.LongToInt      => 5
            case TypeConverter.DoubleToString => 6
            case TypeConverter.StringToDouble => 7
            case TypeConverter.FloatToDouble  => 8
            case TypeConverter.DoubleToFloat  => 9
            case TypeConverter.BoolToString   => 10
            case TypeConverter.StringToBool   => 11
          }
        },
        matchers = Matchers(
          new Matcher[TypeConverter.IntToString.type] {
            def downcastOrNull(a: Any): TypeConverter.IntToString.type = a match {
              case TypeConverter.IntToString => TypeConverter.IntToString
              case _                         => null.asInstanceOf[TypeConverter.IntToString.type]
            }
          },
          new Matcher[TypeConverter.StringToInt.type] {
            def downcastOrNull(a: Any): TypeConverter.StringToInt.type = a match {
              case TypeConverter.StringToInt => TypeConverter.StringToInt
              case _                         => null.asInstanceOf[TypeConverter.StringToInt.type]
            }
          },
          new Matcher[TypeConverter.LongToString.type] {
            def downcastOrNull(a: Any): TypeConverter.LongToString.type = a match {
              case TypeConverter.LongToString => TypeConverter.LongToString
              case _                          => null.asInstanceOf[TypeConverter.LongToString.type]
            }
          },
          new Matcher[TypeConverter.StringToLong.type] {
            def downcastOrNull(a: Any): TypeConverter.StringToLong.type = a match {
              case TypeConverter.StringToLong => TypeConverter.StringToLong
              case _                          => null.asInstanceOf[TypeConverter.StringToLong.type]
            }
          },
          new Matcher[TypeConverter.IntToLong.type] {
            def downcastOrNull(a: Any): TypeConverter.IntToLong.type = a match {
              case TypeConverter.IntToLong => TypeConverter.IntToLong
              case _                       => null.asInstanceOf[TypeConverter.IntToLong.type]
            }
          },
          new Matcher[TypeConverter.LongToInt.type] {
            def downcastOrNull(a: Any): TypeConverter.LongToInt.type = a match {
              case TypeConverter.LongToInt => TypeConverter.LongToInt
              case _                       => null.asInstanceOf[TypeConverter.LongToInt.type]
            }
          },
          new Matcher[TypeConverter.DoubleToString.type] {
            def downcastOrNull(a: Any): TypeConverter.DoubleToString.type = a match {
              case TypeConverter.DoubleToString => TypeConverter.DoubleToString
              case _                            => null.asInstanceOf[TypeConverter.DoubleToString.type]
            }
          },
          new Matcher[TypeConverter.StringToDouble.type] {
            def downcastOrNull(a: Any): TypeConverter.StringToDouble.type = a match {
              case TypeConverter.StringToDouble => TypeConverter.StringToDouble
              case _                            => null.asInstanceOf[TypeConverter.StringToDouble.type]
            }
          },
          new Matcher[TypeConverter.FloatToDouble.type] {
            def downcastOrNull(a: Any): TypeConverter.FloatToDouble.type = a match {
              case TypeConverter.FloatToDouble => TypeConverter.FloatToDouble
              case _                           => null.asInstanceOf[TypeConverter.FloatToDouble.type]
            }
          },
          new Matcher[TypeConverter.DoubleToFloat.type] {
            def downcastOrNull(a: Any): TypeConverter.DoubleToFloat.type = a match {
              case TypeConverter.DoubleToFloat => TypeConverter.DoubleToFloat
              case _                           => null.asInstanceOf[TypeConverter.DoubleToFloat.type]
            }
          },
          new Matcher[TypeConverter.BoolToString.type] {
            def downcastOrNull(a: Any): TypeConverter.BoolToString.type = a match {
              case TypeConverter.BoolToString => TypeConverter.BoolToString
              case _                          => null.asInstanceOf[TypeConverter.BoolToString.type]
            }
          },
          new Matcher[TypeConverter.StringToBool.type] {
            def downcastOrNull(a: Any): TypeConverter.StringToBool.type = a match {
              case TypeConverter.StringToBool => TypeConverter.StringToBool
              case _                          => null.asInstanceOf[TypeConverter.StringToBool.type]
            }
          }
        )
      ),
      modifiers = Chunk.empty
    )
  )
}

/**
 * A single action in a [[DynamicMigration]], operating on a [[DynamicValue]] at
 * a given path.
 *
 * Each action has a structural inverse via `reverse`, allowing round-trip
 * migrations to be derived automatically.
 */
sealed trait MigrationAction {

  /** The path in the [[DynamicValue]] tree this action operates on. */
  def at: DynamicOptic

  /** Returns the structural inverse of this action. */
  def reverse: MigrationAction
}

object MigrationAction {

  /**
   * Inserts a new field at path `at` with the given default value. Reverse is
   * [[DropField]].
   */
  final case class AddField(at: DynamicOptic, default: DynamicValue) extends MigrationAction {
    def reverse: MigrationAction = DropField(at, default)
  }

  /**
   * Removes the field at path `at`. Reverse is [[AddField]] using
   * `defaultForReverse`.
   */
  final case class DropField(at: DynamicOptic, defaultForReverse: DynamicValue) extends MigrationAction {
    def reverse: MigrationAction = AddField(at, defaultForReverse)
  }

  /**
   * Renames a field. The path `at` must end in a [[DynamicOptic.Node.Field]]
   * node whose name is the original field name. Reverse renames `to` back to
   * the original name.
   */
  final case class Rename(at: DynamicOptic, to: String) extends MigrationAction {
    def reverse: MigrationAction = {
      val nodes        = at.nodes
      val originalName = nodes.last.asInstanceOf[DynamicOptic.Node.Field].name
      val parentPath   = new DynamicOptic(nodes.dropRight(1))
      Rename(parentPath.field(to), originalName)
    }
  }

  /**
   * Applies a nested migration to the value at path `at`. Reverse applies
   * `transform.reverse`.
   */
  final case class TransformValue(at: DynamicOptic, transform: DynamicMigration) extends MigrationAction {
    def reverse: MigrationAction = TransformValue(at, transform.reverse)
  }

  /**
   * Makes an optional field (wrapped in `Some`/`None`) required. If the value
   * is `None`, the migration fails. Reverse is [[Optionalize]].
   */
  final case class Mandate(at: DynamicOptic, default: DynamicValue) extends MigrationAction {
    def reverse: MigrationAction = Optionalize(at)
  }

  /**
   * Makes a required field optional by wrapping it in `Some`. Reverse is
   * [[Mandate]] with a `Null` default.
   */
  final case class Optionalize(at: DynamicOptic) extends MigrationAction {
    def reverse: MigrationAction = Mandate(at, DynamicValue.Null)
  }

  /**
   * Changes the primitive type of the value at path `at` using `converter`.
   * Reverse uses the inverse converter.
   */
  final case class ChangeType(at: DynamicOptic, converter: TypeConverter) extends MigrationAction {
    def reverse: MigrationAction = ChangeType(at, converter.reverse)
  }

  /**
   * Renames an enum case. The path `at` must end in a
   * [[DynamicOptic.Node.Case]] node identifying the original case name. Reverse
   * renames `to` back to the original name.
   */
  final case class RenameCase(at: DynamicOptic, to: String) extends MigrationAction {
    def reverse: MigrationAction = {
      val nodes        = at.nodes
      val originalName = nodes.last.asInstanceOf[DynamicOptic.Node.Case].name
      val parentPath   = new DynamicOptic(nodes.dropRight(1))
      RenameCase(parentPath.caseOf(to), originalName)
    }
  }

  /**
   * Applies a sub-migration to the value inside an enum case. The path `at`
   * must end in a [[DynamicOptic.Node.Case]] node. Reverse applies the reversed
   * sub-actions in reverse order.
   */
  final case class TransformCase(at: DynamicOptic, actions: Vector[MigrationAction]) extends MigrationAction {
    def reverse: MigrationAction = TransformCase(at, actions.reverse.map(_.reverse))
  }

  /**
   * Transforms each element of the sequence at path `at`. Reverse applies
   * `transform.reverse`.
   */
  final case class TransformElements(at: DynamicOptic, transform: DynamicMigration) extends MigrationAction {
    def reverse: MigrationAction = TransformElements(at, transform.reverse)
  }

  /**
   * Transforms each key of the map at path `at`. Reverse applies
   * `transform.reverse`.
   */
  final case class TransformKeys(at: DynamicOptic, transform: DynamicMigration) extends MigrationAction {
    def reverse: MigrationAction = TransformKeys(at, transform.reverse)
  }

  /**
   * Transforms each value of the map at path `at`. Reverse applies
   * `transform.reverse`.
   */
  final case class TransformValues(at: DynamicOptic, transform: DynamicMigration) extends MigrationAction {
    def reverse: MigrationAction = TransformValues(at, transform.reverse)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Schema instances
  // ─────────────────────────────────────────────────────────────────────────

  implicit lazy val addFieldSchema: Schema[AddField] = new Schema(
    reflect = new Reflect.Record[Binding, AddField](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Schema[DynamicValue].reflect.asTerm("default")
      ),
      typeId = TypeId.of[AddField],
      recordBinding = new Binding.Record(
        constructor = new Constructor[AddField] {
          def usedRegisters: RegisterOffset                              = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): AddField =
            new AddField(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicValue]
            )
        },
        deconstructor = new Deconstructor[AddField] {
          def usedRegisters: RegisterOffset                                           = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: AddField): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.default)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val dropFieldSchema: Schema[DropField] = new Schema(
    reflect = new Reflect.Record[Binding, DropField](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Schema[DynamicValue].reflect.asTerm("defaultForReverse")
      ),
      typeId = TypeId.of[DropField],
      recordBinding = new Binding.Record(
        constructor = new Constructor[DropField] {
          def usedRegisters: RegisterOffset                               = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): DropField =
            new DropField(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicValue]
            )
        },
        deconstructor = new Deconstructor[DropField] {
          def usedRegisters: RegisterOffset                                            = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: DropField): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.defaultForReverse)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val renameSchema: Schema[Rename] = new Schema(
    reflect = new Reflect.Record[Binding, Rename](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Schema[String].reflect.asTerm("to")
      ),
      typeId = TypeId.of[Rename],
      recordBinding = new Binding.Record(
        constructor = new Constructor[Rename] {
          def usedRegisters: RegisterOffset                            = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): Rename =
            new Rename(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[String]
            )
        },
        deconstructor = new Deconstructor[Rename] {
          def usedRegisters: RegisterOffset                                         = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: Rename): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.to)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val transformValueSchema: Schema[TransformValue] = new Schema(
    reflect = new Reflect.Record[Binding, TransformValue](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Reflect.Deferred(() => DynamicMigration.schema.reflect).asTerm("transform")
      ),
      typeId = TypeId.of[TransformValue],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TransformValue] {
          def usedRegisters: RegisterOffset                                    = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): TransformValue =
            new TransformValue(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicMigration]
            )
        },
        deconstructor = new Deconstructor[TransformValue] {
          def usedRegisters: RegisterOffset                                                 = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: TransformValue): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.transform)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val mandateSchema: Schema[Mandate] = new Schema(
    reflect = new Reflect.Record[Binding, Mandate](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Schema[DynamicValue].reflect.asTerm("default")
      ),
      typeId = TypeId.of[Mandate],
      recordBinding = new Binding.Record(
        constructor = new Constructor[Mandate] {
          def usedRegisters: RegisterOffset                             = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): Mandate =
            new Mandate(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicValue]
            )
        },
        deconstructor = new Deconstructor[Mandate] {
          def usedRegisters: RegisterOffset                                          = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: Mandate): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.default)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val optionalizeSchema: Schema[Optionalize] = new Schema(
    reflect = new Reflect.Record[Binding, Optionalize](
      fields = Chunk.single(Schema[DynamicOptic].reflect.asTerm("at")),
      typeId = TypeId.of[Optionalize],
      recordBinding = new Binding.Record(
        constructor = new Constructor[Optionalize] {
          def usedRegisters: RegisterOffset                                 = RegisterOffset(objects = 1)
          def construct(in: Registers, offset: RegisterOffset): Optionalize =
            new Optionalize(in.getObject(offset).asInstanceOf[DynamicOptic])
        },
        deconstructor = new Deconstructor[Optionalize] {
          def usedRegisters: RegisterOffset                                              = RegisterOffset(objects = 1)
          def deconstruct(out: Registers, offset: RegisterOffset, in: Optionalize): Unit =
            out.setObject(offset, in.at)
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val changeTypeSchema: Schema[ChangeType] = new Schema(
    reflect = new Reflect.Record[Binding, ChangeType](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        TypeConverter.schema.reflect.asTerm("converter")
      ),
      typeId = TypeId.of[ChangeType],
      recordBinding = new Binding.Record(
        constructor = new Constructor[ChangeType] {
          def usedRegisters: RegisterOffset                                = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): ChangeType =
            new ChangeType(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[TypeConverter]
            )
        },
        deconstructor = new Deconstructor[ChangeType] {
          def usedRegisters: RegisterOffset                                             = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: ChangeType): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.converter)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val renameCaseSchema: Schema[RenameCase] = new Schema(
    reflect = new Reflect.Record[Binding, RenameCase](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Schema[String].reflect.asTerm("to")
      ),
      typeId = TypeId.of[RenameCase],
      recordBinding = new Binding.Record(
        constructor = new Constructor[RenameCase] {
          def usedRegisters: RegisterOffset                                = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): RenameCase =
            new RenameCase(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[String]
            )
        },
        deconstructor = new Deconstructor[RenameCase] {
          def usedRegisters: RegisterOffset                                             = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: RenameCase): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.to)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val transformCaseSchema: Schema[TransformCase] = new Schema(
    reflect = new Reflect.Record[Binding, TransformCase](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Reflect.Deferred(() => Schema.vector(MigrationAction.schema).reflect).asTerm("actions")
      ),
      typeId = TypeId.of[TransformCase],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TransformCase] {
          def usedRegisters: RegisterOffset                                   = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): TransformCase =
            new TransformCase(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[Vector[MigrationAction]]
            )
        },
        deconstructor = new Deconstructor[TransformCase] {
          def usedRegisters: RegisterOffset                                                = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: TransformCase): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.actions)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val transformElementsSchema: Schema[TransformElements] = new Schema(
    reflect = new Reflect.Record[Binding, TransformElements](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Reflect.Deferred(() => DynamicMigration.schema.reflect).asTerm("transform")
      ),
      typeId = TypeId.of[TransformElements],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TransformElements] {
          def usedRegisters: RegisterOffset                                       = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): TransformElements =
            new TransformElements(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicMigration]
            )
        },
        deconstructor = new Deconstructor[TransformElements] {
          def usedRegisters: RegisterOffset                                                    = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: TransformElements): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.transform)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val transformKeysSchema: Schema[TransformKeys] = new Schema(
    reflect = new Reflect.Record[Binding, TransformKeys](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Reflect.Deferred(() => DynamicMigration.schema.reflect).asTerm("transform")
      ),
      typeId = TypeId.of[TransformKeys],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TransformKeys] {
          def usedRegisters: RegisterOffset                                   = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): TransformKeys =
            new TransformKeys(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicMigration]
            )
        },
        deconstructor = new Deconstructor[TransformKeys] {
          def usedRegisters: RegisterOffset                                                = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: TransformKeys): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.transform)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val transformValuesSchema: Schema[TransformValues] = new Schema(
    reflect = new Reflect.Record[Binding, TransformValues](
      fields = Chunk(
        Schema[DynamicOptic].reflect.asTerm("at"),
        Reflect.Deferred(() => DynamicMigration.schema.reflect).asTerm("transform")
      ),
      typeId = TypeId.of[TransformValues],
      recordBinding = new Binding.Record(
        constructor = new Constructor[TransformValues] {
          def usedRegisters: RegisterOffset                                     = RegisterOffset(objects = 2)
          def construct(in: Registers, offset: RegisterOffset): TransformValues =
            new TransformValues(
              in.getObject(offset).asInstanceOf[DynamicOptic],
              in.getObject(RegisterOffset.incrementObjects(offset)).asInstanceOf[DynamicMigration]
            )
        },
        deconstructor = new Deconstructor[TransformValues] {
          def usedRegisters: RegisterOffset                                                  = RegisterOffset(objects = 2)
          def deconstruct(out: Registers, offset: RegisterOffset, in: TransformValues): Unit = {
            out.setObject(offset, in.at)
            out.setObject(RegisterOffset.incrementObjects(offset), in.transform)
          }
        }
      ),
      modifiers = Chunk.empty
    )
  )

  implicit lazy val schema: Schema[MigrationAction] = new Schema(
    reflect = new Reflect.Variant[Binding, MigrationAction](
      cases = Chunk(
        addFieldSchema.reflect.asTerm("AddField"),
        dropFieldSchema.reflect.asTerm("DropField"),
        renameSchema.reflect.asTerm("Rename"),
        Reflect.Deferred(() => transformValueSchema.reflect).asTerm("TransformValue"),
        mandateSchema.reflect.asTerm("Mandate"),
        optionalizeSchema.reflect.asTerm("Optionalize"),
        changeTypeSchema.reflect.asTerm("ChangeType"),
        renameCaseSchema.reflect.asTerm("RenameCase"),
        Reflect.Deferred(() => transformCaseSchema.reflect).asTerm("TransformCase"),
        Reflect.Deferred(() => transformElementsSchema.reflect).asTerm("TransformElements"),
        Reflect.Deferred(() => transformKeysSchema.reflect).asTerm("TransformKeys"),
        Reflect.Deferred(() => transformValuesSchema.reflect).asTerm("TransformValues")
      ),
      typeId = TypeId.of[MigrationAction],
      variantBinding = new Binding.Variant(
        discriminator = new Discriminator[MigrationAction] {
          def discriminate(a: MigrationAction): Int = a match {
            case _: AddField          => 0
            case _: DropField         => 1
            case _: Rename            => 2
            case _: TransformValue    => 3
            case _: Mandate           => 4
            case _: Optionalize       => 5
            case _: ChangeType        => 6
            case _: RenameCase        => 7
            case _: TransformCase     => 8
            case _: TransformElements => 9
            case _: TransformKeys     => 10
            case _: TransformValues   => 11
          }
        },
        matchers = Matchers(
          new Matcher[AddField] {
            def downcastOrNull(a: Any): AddField = a match {
              case x: AddField => x
              case _           => null.asInstanceOf[AddField]
            }
          },
          new Matcher[DropField] {
            def downcastOrNull(a: Any): DropField = a match {
              case x: DropField => x
              case _            => null.asInstanceOf[DropField]
            }
          },
          new Matcher[Rename] {
            def downcastOrNull(a: Any): Rename = a match {
              case x: Rename => x
              case _         => null.asInstanceOf[Rename]
            }
          },
          new Matcher[TransformValue] {
            def downcastOrNull(a: Any): TransformValue = a match {
              case x: TransformValue => x
              case _                 => null.asInstanceOf[TransformValue]
            }
          },
          new Matcher[Mandate] {
            def downcastOrNull(a: Any): Mandate = a match {
              case x: Mandate => x
              case _          => null.asInstanceOf[Mandate]
            }
          },
          new Matcher[Optionalize] {
            def downcastOrNull(a: Any): Optionalize = a match {
              case x: Optionalize => x
              case _              => null.asInstanceOf[Optionalize]
            }
          },
          new Matcher[ChangeType] {
            def downcastOrNull(a: Any): ChangeType = a match {
              case x: ChangeType => x
              case _             => null.asInstanceOf[ChangeType]
            }
          },
          new Matcher[RenameCase] {
            def downcastOrNull(a: Any): RenameCase = a match {
              case x: RenameCase => x
              case _             => null.asInstanceOf[RenameCase]
            }
          },
          new Matcher[TransformCase] {
            def downcastOrNull(a: Any): TransformCase = a match {
              case x: TransformCase => x
              case _                => null.asInstanceOf[TransformCase]
            }
          },
          new Matcher[TransformElements] {
            def downcastOrNull(a: Any): TransformElements = a match {
              case x: TransformElements => x
              case _                    => null.asInstanceOf[TransformElements]
            }
          },
          new Matcher[TransformKeys] {
            def downcastOrNull(a: Any): TransformKeys = a match {
              case x: TransformKeys => x
              case _                => null.asInstanceOf[TransformKeys]
            }
          },
          new Matcher[TransformValues] {
            def downcastOrNull(a: Any): TransformValues = a match {
              case x: TransformValues => x
              case _                  => null.asInstanceOf[TransformValues]
            }
          }
        )
      ),
      modifiers = Chunk.empty
    )
  )
}
