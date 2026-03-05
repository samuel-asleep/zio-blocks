package zio.blocks.schema.migration

import zio.blocks.schema._
import zio.test._

object MigrationSpec extends SchemaBaseSpec {

  case class PersonV1(name: String, age: Int)
  case class PersonV2(name: String, age: String, active: Boolean)
  case class PersonV3(firstName: String, age: String, active: Boolean)

  implicit val personV1Schema: Schema[PersonV1] = Schema.derived
  implicit val personV2Schema: Schema[PersonV2] = Schema.derived
  implicit val personV3Schema: Schema[PersonV3] = Schema.derived

  private val v1ToV2: Migration[PersonV1, PersonV2] =
    Migration
      .builder[PersonV1, PersonV2]
      .changeType(DynamicOptic.root.field("age"), TypeConverter.IntToString)
      .addField(DynamicOptic.root.field("active"), DynamicValue.Primitive(PrimitiveValue.Boolean(true)))
      .build

  private val v2ToV3: Migration[PersonV2, PersonV3] =
    Migration
      .builder[PersonV2, PersonV3]
      .rename(DynamicOptic.root.field("name"), "firstName")
      .build

  def spec: Spec[Any, Any] = suite("MigrationSpec")(
    suite("apply")(
      test("migrates PersonV1 to PersonV2") {
        val input    = PersonV1("Alice", 30)
        val expected = PersonV2("Alice", "30", true)
        assertTrue(v1ToV2(input) == Right(expected))
      },
      test("migrates PersonV2 to PersonV3") {
        val input    = PersonV2("Bob", "25", false)
        val expected = PersonV3("Bob", "25", false)
        assertTrue(v2ToV3(input) == Right(expected))
      }
    ),

    suite("andThen / compose")(
      test("composes migrations v1 -> v2 -> v3") {
        val v1ToV3   = v1ToV2.andThen(v2ToV3)
        val input    = PersonV1("Charlie", 40)
        val expected = PersonV3("Charlie", "40", true)
        assertTrue(v1ToV3(input) == Right(expected))
      }
    ),

    suite("reverse")(
      test("round-trips via reverse when possible") {
        // Use a self-migration that only changes age type back and forth
        case class NumBox(n: Int)
        case class StrBox(n: String)
        implicit val numSchema: Schema[NumBox] = Schema.derived
        implicit val strSchema: Schema[StrBox] = Schema.derived
        val forward                            = Migration
          .builder[NumBox, StrBox]
          .changeType(DynamicOptic.root.field("n"), TypeConverter.IntToString)
          .build
        val rev      = forward.reverse
        val input    = NumBox(42)
        val migrated = forward(input)
        assertTrue(migrated.flatMap(rev(_)) == Right(input))
      }
    ),

    suite("identity")(
      test("Migration.identity leaves value unchanged") {
        val m     = Migration.identity[PersonV1]
        val input = PersonV1("Diana", 28)
        assertTrue(m(input) == Right(input))
      }
    ),

    suite("builder")(
      test("builder.build produces correct migration") {
        val m = Migration
          .builder[PersonV1, PersonV1]
          .changeType(DynamicOptic.root.field("age"), TypeConverter.IntToString)
          .changeType(DynamicOptic.root.field("age"), TypeConverter.StringToInt)
          .build
        val input = PersonV1("Eve", 99)
        assertTrue(m(input) == Right(input))
      }
    )
  )
}
