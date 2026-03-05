package zio.blocks.schema.migration

import zio.blocks.chunk.Chunk
import zio.blocks.schema._
import zio.test._

/**
 * Laws for [[DynamicMigration]] and [[Migration]].
 *
 * Verifies: identity, associativity of composition, reverse round-trip.
 */
object MigrationLawsSpec extends SchemaBaseSpec {

  private def prim(n: Int): DynamicValue     = DynamicValue.Primitive(PrimitiveValue.Int(n))
  private def primS(s: String): DynamicValue = DynamicValue.Primitive(PrimitiveValue.String(s))
  private def record(fields: (String, DynamicValue)*): DynamicValue =
    DynamicValue.Record(Chunk.from(fields))

  private val value1 = record("n" -> prim(1), "s" -> primS("hello"))

  private val intToString = DynamicMigration(Vector(
    MigrationAction.ChangeType(DynamicOptic.root.field("n"), TypeConverter.IntToString)
  ))

  def spec: Spec[Any, Any] = suite("MigrationLawsSpec")(

    suite("identity laws")(
      test("left identity: identity ++ m == m") {
        val m        = intToString
        val combined = DynamicMigration.identity ++ m
        assertTrue(combined(value1) == m(value1))
      },
      test("right identity: m ++ identity == m") {
        val m        = intToString
        val combined = m ++ DynamicMigration.identity
        assertTrue(combined(value1) == m(value1))
      },
      test("identity migration is identity function") {
        assertTrue(DynamicMigration.identity(value1) == Right(value1))
      }
    ),

    suite("composition associativity")(
      test("(a ++ b) ++ c == a ++ (b ++ c)") {
        val renameN  = DynamicMigration(Vector(
          MigrationAction.Rename(DynamicOptic.root.field("n"), "num")
        ))
        val addY     = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("y"), prim(99))
        ))
        val lhs = (intToString ++ renameN) ++ addY
        val rhs = intToString ++ (renameN ++ addY)
        assertTrue(lhs(value1) == rhs(value1))
      }
    ),

    suite("reverse laws")(
      test("reverse . forward == identity for AddField/DropField pair") {
        val addField = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("z"), prim(0))
        ))
        val before   = record("n" -> prim(1))
        val after    = addField(before)
        val reversed = addField.reverse
        assertTrue(after.flatMap(reversed(_)) == Right(before))
      },
      test("reverse . forward == identity for Rename") {
        // Rename moves the field to the end, so compare sorted fields
        val renameMig = DynamicMigration(Vector(
          MigrationAction.Rename(DynamicOptic.root.field("n"), "num")
        ))
        val after    = renameMig(value1)
        val reversed = renameMig.reverse
        val result   = after.flatMap(reversed(_))
        // Field order may change after rename; compare normalized
        assertTrue(result.map(_.sortFields) == Right(value1.sortFields))
      },
      test("reverse . forward == identity for ChangeType") {
        val after    = intToString(value1)
        val reversed = intToString.reverse
        assertTrue(after.flatMap(reversed(_)) == Right(value1))
      },
      test("reverse . forward == identity for RenameCase") {
        val variantVal = record("dir" -> DynamicValue.Variant("North", DynamicValue.Null))
        val renameMig  = DynamicMigration(Vector(
          MigrationAction.RenameCase(DynamicOptic.root.field("dir").caseOf("North"), "Up")
        ))
        val after    = renameMig(variantVal)
        val reversed = renameMig.reverse
        assertTrue(after.flatMap(reversed(_)) == Right(variantVal))
      },
      test("reverse of identity is identity") {
        val rev = DynamicMigration.identity.reverse
        assertTrue(rev(value1) == Right(value1))
      },
      test("reverse of composition reverses order") {
        val before    = record("n" -> prim(5))
        val addExtra2 = DynamicMigration(Vector(
          MigrationAction.AddField(DynamicOptic.root.field("extra"), primS("x"))
        ))
        val combined  = intToString ++ addExtra2
        val migrated  = combined(before)
        val rev       = combined.reverse
        assertTrue(migrated.flatMap(rev(_)) == Right(before))
      }
    ),

    suite("isEmpty")(
      test("identity is empty") {
        assertTrue(DynamicMigration.identity.isEmpty)
      },
      test("non-empty migration is not empty") {
        assertTrue(!intToString.isEmpty)
      }
    ),

    suite("Migration.identity laws")(
      test("Migration.identity leaves typed value unchanged") {
        case class Foo(x: Int)
        implicit val fooSchema: Schema[Foo] = Schema.derived
        val m     = Migration.identity[Foo]
        val input = Foo(42)
        assertTrue(m(input) == Right(input))
      }
    )
  )
}
