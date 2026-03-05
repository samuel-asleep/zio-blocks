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
    ),

    suite("builder - all methods")(
      test("dropField builder method") {
        case class WithExtra(name: String, age: Int, extra: String)
        implicit val withExtraSchema: Schema[WithExtra] = Schema.derived
        val m                                           = Migration
          .builder[WithExtra, PersonV1]
          .dropField(DynamicOptic.root.field("extra"), DynamicValue.Primitive(PrimitiveValue.String("")))
          .build
        assertTrue(m(WithExtra("Alice", 30, "x")) == Right(PersonV1("Alice", 30)))
      },
      test("transformValue builder method") {
        val subMig = DynamicMigration(
          Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString))
        )
        val m = Migration
          .builder[PersonV1, PersonV2]
          .transformValue(DynamicOptic.root.field("age"), subMig)
          .addField(
            DynamicOptic.root.field("active"),
            DynamicValue.Primitive(PrimitiveValue.Boolean(false))
          )
          .build
        assertTrue(m(PersonV1("Bob", 25)).map(_.age) == Right("25"))
      },
      test("mandate builder method") {
        case class OptBox(value: Option[Int])
        case class ReqBox(value: Int)
        implicit val optSchema: Schema[OptBox] = Schema.derived
        implicit val reqSchema: Schema[ReqBox] = Schema.derived
        val m                                  = Migration
          .builder[OptBox, ReqBox]
          .mandate(DynamicOptic.root.field("value"), DynamicValue.Null)
          .build
        assertTrue(m(OptBox(Some(42))) == Right(ReqBox(42)))
      },
      test("optionalize builder method") {
        case class ReqBox(value: Int)
        case class OptBox(value: Option[Int])
        implicit val reqSchema: Schema[ReqBox] = Schema.derived
        implicit val optSchema: Schema[OptBox] = Schema.derived
        val m                                  = Migration
          .builder[ReqBox, OptBox]
          .optionalize(DynamicOptic.root.field("value"))
          .build
        assertTrue(m(ReqBox(7)) == Right(OptBox(Some(7))))
      },
      test("renameCase builder method") {
        sealed trait DirectionV1
        case object NorthV1 extends DirectionV1
        case object SouthV1 extends DirectionV1
        sealed trait DirectionV2
        case object UpV2    extends DirectionV2
        case object SouthV2 extends DirectionV2
        implicit val dirV1Schema: Schema[DirectionV1] = Schema.derived
        implicit val dirV2Schema: Schema[DirectionV2] = Schema.derived
        val m                                         = Migration
          .builder[DirectionV1, DirectionV2]
          .renameCase(DynamicOptic.root.caseOf("NorthV1"), "UpV2")
          .build
        assertTrue(m(NorthV1).isRight)
      },
      test("transformElements builder method") {
        case class NumList(items: List[Int])
        case class StrList(items: List[String])
        implicit val numListSchema: Schema[NumList] = Schema.derived
        implicit val strListSchema: Schema[StrList] = Schema.derived
        val elemMig                                 = DynamicMigration(
          Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString))
        )
        val m = Migration
          .builder[NumList, StrList]
          .transformElements(DynamicOptic.root.field("items"), elemMig)
          .build
        assertTrue(m(NumList(List(1, 2, 3))) == Right(StrList(List("1", "2", "3"))))
      },
      test("transformKeys builder method") {
        case class IntKeyMap(entries: Map[Int, String])
        case class StrKeyMap(entries: Map[String, String])
        implicit val intMapSchema: Schema[IntKeyMap] = Schema.derived
        implicit val strMapSchema: Schema[StrKeyMap] = Schema.derived
        val keyMig                                   = DynamicMigration(
          Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString))
        )
        val m = Migration
          .builder[IntKeyMap, StrKeyMap]
          .transformKeys(DynamicOptic.root.field("entries"), keyMig)
          .build
        assertTrue(m(IntKeyMap(Map(1 -> "a"))).isRight)
      },
      test("transformValues builder method") {
        case class IntValMap(entries: Map[String, Int])
        case class StrValMap(entries: Map[String, String])
        implicit val intMapSchema: Schema[IntValMap] = Schema.derived
        implicit val strMapSchema: Schema[StrValMap] = Schema.derived
        val valMig                                   = DynamicMigration(
          Vector(MigrationAction.ChangeType(DynamicOptic.root, TypeConverter.IntToString))
        )
        val m = Migration
          .builder[IntValMap, StrValMap]
          .transformValues(DynamicOptic.root.field("entries"), valMig)
          .build
        assertTrue(m(IntValMap(Map("k" -> 1))).isRight)
      },
      test("transformCase builder method") {
        sealed trait StatusV1
        case class OkV1(code: Int)    extends StatusV1
        case class ErrV1(msg: String) extends StatusV1
        sealed trait StatusV2
        case class OkV2(code: Int, note: String) extends StatusV2
        case class ErrV2(msg: String)            extends StatusV2
        implicit val v1Schema: Schema[StatusV1] = Schema.derived
        implicit val v2Schema: Schema[StatusV2] = Schema.derived
        val subActions                          = Vector(
          MigrationAction.AddField(DynamicOptic.root.field("note"), DynamicValue.Primitive(PrimitiveValue.String("")))
        )
        val m = Migration
          .builder[StatusV1, StatusV2]
          .transformCase(DynamicOptic.root.caseOf("OkV1"), subActions)
          .build
        assertTrue(m(OkV1(200)).isRight)
      }
    )
  )
}
