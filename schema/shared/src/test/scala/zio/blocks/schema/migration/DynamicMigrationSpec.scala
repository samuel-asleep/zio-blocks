package zio.blocks.schema.migration

import zio.blocks.chunk.Chunk
import zio.blocks.schema._
import zio.test._

object DynamicMigrationSpec extends SchemaBaseSpec {

  private def prim(n: Int): DynamicValue       = DynamicValue.Primitive(PrimitiveValue.Int(n))
  private def primL(n: Long): DynamicValue     = DynamicValue.Primitive(PrimitiveValue.Long(n))
  private def primS(s: String): DynamicValue   = DynamicValue.Primitive(PrimitiveValue.String(s))
  private def primD(d: Double): DynamicValue   = DynamicValue.Primitive(PrimitiveValue.Double(d))
  private def primF(f: Float): DynamicValue    = DynamicValue.Primitive(PrimitiveValue.Float(f))
  private def primB(b: Boolean): DynamicValue  = DynamicValue.Primitive(PrimitiveValue.Boolean(b))

  private def record(fields: (String, DynamicValue)*): DynamicValue =
    DynamicValue.Record(Chunk.from(fields))

  private def variant(name: String, value: DynamicValue): DynamicValue =
    DynamicValue.Variant(name, value)

  private def seq(elems: DynamicValue*): DynamicValue =
    DynamicValue.Sequence(Chunk.from(elems))

  private def dmap(entries: (DynamicValue, DynamicValue)*): DynamicValue =
    DynamicValue.Map(Chunk.from(entries))

  def spec: Spec[Any, Any] = suite("DynamicMigrationSpec")(

    suite("identity migration")(
      test("leaves any value unchanged") {
        val value = record("x" -> prim(1), "y" -> primS("hello"))
        val result = DynamicMigration.identity(value)
        assertTrue(result == Right(value))
      }
    ),

    suite("AddField")(
      test("inserts a new field with the default value") {
        val before   = record("name" -> primS("Alice"))
        val after    = record("name" -> primS("Alice"), "age" -> prim(0))
        val migration = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("age"), prim(0))
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("fails if the field already exists") {
        val before   = record("name" -> primS("Alice"), "age" -> prim(30))
        val migration = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("age"), prim(0))
        ))
        assertTrue(migration(before).isLeft)
      }
    ),

    suite("DropField")(
      test("removes an existing field") {
        val before   = record("name" -> primS("Alice"), "age" -> prim(30))
        val after    = record("name" -> primS("Alice"))
        val migration = DynamicMigration(Vector(
          MigrationAction.DropField(DynamicOptic.root.field("age"), prim(0))
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("fails if the field does not exist") {
        val before   = record("name" -> primS("Alice"))
        val migration = DynamicMigration(Vector(
          MigrationAction.DropField(DynamicOptic.root.field("age"), prim(0))
        ))
        assertTrue(migration(before).isLeft)
      }
    ),

    suite("Rename")(
      test("renames a top-level field") {
        val before    = record("firstName" -> primS("Alice"))
        val after     = record("first_name" -> primS("Alice"))
        val migration = DynamicMigration(Vector(
          MigrationAction.Rename(DynamicOptic.root.field("firstName"), "first_name")
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("renames a nested field") {
        val before    = record("address" -> record("streetName" -> primS("Main St")))
        val after     = record("address" -> record("street" -> primS("Main St")))
        val migration = DynamicMigration(Vector(
          MigrationAction.Rename(DynamicOptic.root.field("address").field("streetName"), "street")
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("reverse renames back") {
        val original  = record("firstName" -> primS("Alice"))
        val action    = MigrationAction.Rename(DynamicOptic.root.field("firstName"), "first_name")
        val migrated  = record("first_name" -> primS("Alice"))
        val rev       = DynamicMigration(Vector(action.reverse))
        assertTrue(rev(migrated) == Right(original))
      }
    ),

    suite("TransformValue")(
      test("applies a nested migration to a field value") {
        val subMig    = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val before2   = record("n" -> prim(42))
        val after2    = record("n" -> primS("42"))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformValue(DynamicOptic.root.field("n"), subMig)
        ))
        assertTrue(migration(before2) == Right(after2))
      },
      test("applies migration to root") {
        val before    = prim(10)
        val subMig    = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformValue(DynamicOptic.root, subMig)
        ))
        assertTrue(migration(before) == Right(primS("10")))
      }
    ),

    suite("Mandate")(
      test("unwraps Some") {
        val before    = record("score" -> variant("Some", prim(99)))
        val after     = record("score" -> prim(99))
        val migration = DynamicMigration(Vector(
          MigrationAction.Mandate(DynamicOptic.root.field("score"), DynamicValue.Null)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("fails on None") {
        val before    = record("score" -> variant("None", DynamicValue.Null))
        val migration = DynamicMigration(Vector(
          MigrationAction.Mandate(DynamicOptic.root.field("score"), DynamicValue.Null)
        ))
        assertTrue(migration(before).isLeft)
      }
    ),

    suite("Optionalize")(
      test("wraps value in Some") {
        val before    = record("score" -> prim(99))
        val after     = record("score" -> variant("Some", prim(99)))
        val migration = DynamicMigration(Vector(
          MigrationAction.Optionalize(DynamicOptic.root.field("score"))
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("reverse of Optionalize is Mandate") {
        val before    = record("score" -> prim(99))
        val opt       = MigrationAction.Optionalize(DynamicOptic.root.field("score"))
        val optMig    = DynamicMigration(Vector(opt))
        val mandMig   = DynamicMigration(Vector(opt.reverse))
        val optResult = optMig(before)
        assertTrue(optResult.flatMap(mandMig(_)) == Right(before))
      }
    ),

    suite("ChangeType")(
      test("IntToString") {
        val before    = record("n" -> prim(42))
        val after     = record("n" -> primS("42"))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.IntToString)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("StringToInt") {
        val before    = record("n" -> primS("42"))
        val after     = record("n" -> prim(42))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.StringToInt)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("IntToLong") {
        val before    = record("n" -> prim(100))
        val after     = record("n" -> primL(100L))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.IntToLong)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("LongToInt") {
        val before    = record("n" -> primL(100L))
        val after     = record("n" -> prim(100))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.LongToInt)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("FloatToDouble") {
        val before    = record("x" -> primF(1.5f))
        val after     = record("x" -> primD(1.5f.toDouble))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("x"), TypeConverter.FloatToDouble)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("BoolToString") {
        val before    = record("flag" -> primB(true))
        val after     = record("flag" -> primS("true"))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("flag"), TypeConverter.BoolToString)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("StringToBool true/false") {
        val t = DynamicMigration(Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.StringToBool)))
        val f = DynamicMigration(Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.StringToBool)))
        assertTrue(
          t(primS("true"))  == Right(primB(true)) &&
          f(primS("false")) == Right(primB(false))
        )
      },
      test("StringToBool invalid value fails") {
        val m = DynamicMigration(Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.StringToBool)))
        assertTrue(m(primS("yes")).isLeft)
      },
      test("StringToInt invalid value fails") {
        val m = DynamicMigration(Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.StringToInt)))
        assertTrue(m(primS("notanumber")).isLeft)
      }
    ),

    suite("RenameCase")(
      test("renames an enum case") {
        val before    = record("dir" -> variant("North", DynamicValue.Null))
        val after     = record("dir" -> variant("Up", DynamicValue.Null))
        val migration = DynamicMigration(Vector(
          MigrationAction.RenameCase(DynamicOptic.root.field("dir").caseOf("North"), "Up")
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("fails if case name does not match") {
        val before    = record("dir" -> variant("South", DynamicValue.Null))
        val migration = DynamicMigration(Vector(
          MigrationAction.RenameCase(DynamicOptic.root.field("dir").caseOf("North"), "Up")
        ))
        assertTrue(migration(before).isLeft)
      },
      test("reverse renames back") {
        val original  = record("dir" -> variant("North", DynamicValue.Null))
        val action    = MigrationAction.RenameCase(DynamicOptic.root.field("dir").caseOf("North"), "Up")
        val migrated  = record("dir" -> variant("Up", DynamicValue.Null))
        val rev       = DynamicMigration(Vector(action.reverse))
        assertTrue(rev(migrated) == Right(original))
      }
    ),

    suite("TransformCase")(
      test("applies sub-actions to a matching case value") {
        val before    = record("result" -> variant("Success", record("code" -> prim(200))))
        val after     = record("result" -> variant("Success", record("code" -> prim(200), "msg" -> primS("ok"))))
        val subActions = Vector(
          MigrationAction.AddField(DynamicOptic.root.field("msg"), primS("ok"))
        )
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformCase(DynamicOptic.root.field("result").caseOf("Success"), subActions)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("skips if case name does not match") {
        val before = record("result" -> variant("Failure", record("err" -> primS("bad"))))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformCase(
            DynamicOptic.root.field("result").caseOf("Success"),
            Vector(MigrationAction.AddField(DynamicOptic.root.field("msg"), primS("ok")))
          )
        ))
        assertTrue(migration(before).isLeft)
      }
    ),

    suite("TransformElements")(
      test("transforms each element of a sequence") {
        val before    = record("nums" -> seq(prim(1), prim(2), prim(3)))
        val after     = record("nums" -> seq(primS("1"), primS("2"), primS("3")))
        val elemMig   = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformElements(DynamicOptic.root.field("nums"), elemMig)
        ))
        assertTrue(migration(before) == Right(after))
      },
      test("fails if path does not hold a Sequence") {
        val before    = record("n" -> prim(42))
        val elemMig   = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformElements(DynamicOptic.root.field("n"), elemMig)
        ))
        assertTrue(migration(before).isLeft)
      }
    ),

    suite("TransformKeys")(
      test("transforms each key of a map") {
        val before    = dmap(prim(1) -> primS("a"), prim(2) -> primS("b"))
        val after     = dmap(primS("1") -> primS("a"), primS("2") -> primS("b"))
        val keyMig    = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformKeys(DynamicOptic.root, keyMig)
        ))
        assertTrue(migration(before) == Right(after))
      }
    ),

    suite("TransformValues")(
      test("transforms each value of a map") {
        val before    = dmap(primS("a") -> prim(1), primS("b") -> prim(2))
        val after     = dmap(primS("a") -> primS("1"), primS("b") -> primS("2"))
        val valMig    = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        ))
        val migration = DynamicMigration(Vector(
          MigrationAction.TransformValues(DynamicOptic.root, valMig)
        ))
        assertTrue(migration(before) == Right(after))
      }
    ),

    suite("composition")(
      test("composes two migrations sequentially") {
        val before    = record("n" -> prim(1))
        val step1     = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.IntToString)
        ))
        val step2     = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("extra"), prim(0))
        ))
        val combined  = step1 ++ step2
        val expected  = record("n" -> primS("1"), "extra" -> prim(0))
        assertTrue(combined(before) == Right(expected))
      }
    ),

    suite("reverse")(
      test("reverse of AddField is DropField") {
        val action  = MigrationAction.AddField(DynamicOptic.root.field("x"), prim(0))
        val before  = record("a" -> primS("val"))
        val forward = DynamicMigration(Vector(action))
        val rev     = DynamicMigration(Vector(action.reverse))
        assertTrue(forward(before).flatMap(rev(_)) == Right(before))
      },
      test("reverse of DropField is AddField") {
        val action  = MigrationAction.DropField(DynamicOptic.root.field("x"), prim(0))
        val before  = record("a" -> primS("val"), "x" -> prim(0))
        val forward = DynamicMigration(Vector(action))
        val rev     = DynamicMigration(Vector(action.reverse))
        assertTrue(forward(before).flatMap(rev(_)) == Right(before))
      },
      test("reverse of ChangeType roundtrips") {
        val action  = MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString)
        val before  = prim(42)
        val forward = DynamicMigration(Vector(action))
        val rev     = DynamicMigration(Vector(action.reverse))
        assertTrue(forward(before).flatMap(rev(_)) == Right(before))
      },
      test("DynamicMigration.reverse reverses the whole migration") {
        val before   = record("n" -> prim(42))
        val migration = DynamicMigration(Vector(
          MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.IntToString)
        ))
        val migrated = migration(before)
        val reversed = migration.reverse
        assertTrue(migrated.flatMap(reversed(_)) == Right(before))
      }
    ),

    suite("serialization roundtrip")(
      test("DynamicMigration schema roundtrips") {
        val migration = DynamicMigration(Vector(
          MigrationAction.Rename(DynamicOptic.root.field("firstName"), "first_name"),
          MigrationAction.ChangeType(DynamicOptic.root.field("age"), TypeConverter.IntToString)
        ))
        val dv       = DynamicMigration.schema.toDynamicValue(migration)
        val restored = DynamicMigration.schema.fromDynamicValue(dv)
        assertTrue(restored == Right(migration))
      },
      test("TypeConverter schema roundtrips") {
        val converters: Vector[TypeConverter] = Vector(
          TypeConverter.IntToString,
          TypeConverter.StringToInt,
          TypeConverter.LongToString,
          TypeConverter.StringToLong,
          TypeConverter.IntToLong,
          TypeConverter.LongToInt,
          TypeConverter.DoubleToString,
          TypeConverter.StringToDouble,
          TypeConverter.FloatToDouble,
          TypeConverter.DoubleToFloat,
          TypeConverter.BoolToString,
          TypeConverter.StringToBool
        )
        val results = converters.map { c =>
          val dv = TypeConverter.schema.toDynamicValue(c)
          TypeConverter.schema.fromDynamicValue(dv)
        }
        assertTrue(results.forall(_.isRight))
      }
    )
  )
}
