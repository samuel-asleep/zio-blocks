package zio.blocks.schema.json

import zio.blocks.schema.DynamicOptic
import zio.blocks.schema.json.JsonPatch._
import zio.test._

object JsonPatchSpec extends SchemaBaseSpec {

  def spec: Spec[TestEnvironment, Any] = suite("JsonPatchSpec")(
    suite("operation types")(
      test("Set replaces the target value") {
        val patch = JsonPatch.root(Op.Set(Json.String("updated")))
        assertTrue(patch(Json.String("old")) == Right(Json.String("updated")))
      },
      test("PrimitiveDelta applies NumberDelta") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(2))))
        assertTrue(patch(Json.Number("3")) == Right(Json.Number("5")))
      },
      test("PrimitiveDelta applies StringEdit") {
        val patch = JsonPatch.root(
          Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Insert(1, "X"))))
        )
        assertTrue(patch(Json.String("abc")) == Right(Json.String("aXbc")))
      },
      test("ArrayEdit applies sequence edits") {
        val patch = JsonPatch.root(
          Op.ArrayEdit(Vector(ArrayOp.Append(Vector(Json.Number("2")))))
        )
        assertTrue(
          patch(Json.Array(Json.Number("1"))) == Right(Json.Array(Json.Number("1"), Json.Number("2")))
        )
      },
      test("ObjectEdit applies map edits") {
        val patch = JsonPatch.root(
          Op.ObjectEdit(Vector(ObjectOp.Add("b", Json.Number("2"))))
        )
        assertTrue(
          patch(Json.Object("a" -> Json.Number("1"))) ==
            Right(Json.Object("a" -> Json.Number("1"), "b" -> Json.Number("2")))
        )
      },
      test("Nested applies a nested patch") {
        val nested = JsonPatch(Vector(JsonPatchOp(DynamicOptic.root.field("a"), Op.Set(Json.Number("2")))))
        val patch  = JsonPatch.root(Op.Nested(nested))
        val json   = Json.Object("a" -> Json.Number("1"))
        assertTrue(patch(json) == Right(Json.Object("a" -> Json.Number("2"))))
      }
    ),
    suite("string operations")(
      test("Insert") {
        val patch = JsonPatch.root(
          Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Insert(1, "Z"))))
        )
        assertTrue(patch(Json.String("ab")) == Right(Json.String("aZb")))
      },
      test("Delete") {
        val patch = JsonPatch.root(
          Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Delete(1, 2))))
        )
        assertTrue(patch(Json.String("abcd")) == Right(Json.String("ad")))
      },
      test("Append") {
        val patch = JsonPatch.root(
          Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Append("!"))))
        )
        assertTrue(patch(Json.String("hi")) == Right(Json.String("hi!")))
      },
      test("Modify") {
        val patch = JsonPatch.root(
          Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Modify(1, 2, "Z"))))
        )
        assertTrue(patch(Json.String("abcd")) == Right(Json.String("aZd")))
      }
    ),
    suite("array operations")(
      test("Insert") {
        val patch = JsonPatch.root(
          Op.ArrayEdit(Vector(ArrayOp.Insert(1, Vector(Json.Number("2")))))
        )
        val json = Json.Array(Json.Number("1"), Json.Number("3"))
        assertTrue(patch(json) == Right(Json.Array(Json.Number("1"), Json.Number("2"), Json.Number("3"))))
      },
      test("Append") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Append(Vector(Json.Number("2"))))))
        val json  = Json.Array(Json.Number("1"))
        assertTrue(patch(json) == Right(Json.Array(Json.Number("1"), Json.Number("2"))))
      },
      test("Delete") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Delete(1, 1))))
        val json  = Json.Array(Json.Number("1"), Json.Number("2"), Json.Number("3"))
        assertTrue(patch(json) == Right(Json.Array(Json.Number("1"), Json.Number("3"))))
      },
      test("Modify") {
        val patch = JsonPatch.root(
          Op.ArrayEdit(Vector(ArrayOp.Modify(1, Op.Set(Json.String("x")))))
        )
        val json = Json.Array(Json.String("a"), Json.String("b"))
        assertTrue(patch(json) == Right(Json.Array(Json.String("a"), Json.String("x"))))
      }
    ),
    suite("object operations")(
      test("Add") {
        val patch = JsonPatch.root(
          Op.ObjectEdit(Vector(ObjectOp.Add("b", Json.Number("2"))))
        )
        val json = Json.Object("a" -> Json.Number("1"))
        assertTrue(patch(json) == Right(Json.Object("a" -> Json.Number("1"), "b" -> Json.Number("2"))))
      },
      test("Remove") {
        val patch = JsonPatch.root(
          Op.ObjectEdit(Vector(ObjectOp.Remove("b")))
        )
        val json = Json.Object("a" -> Json.Number("1"), "b" -> Json.Number("2"))
        assertTrue(patch(json) == Right(Json.Object("a" -> Json.Number("1"))))
      },
      test("Modify") {
        val patch = JsonPatch.root(
          Op.ObjectEdit(Vector(ObjectOp.Modify("b", JsonPatch.root(Op.Set(Json.Number("3"))))))
        )
        val json = Json.Object("b" -> Json.Number("2"))
        assertTrue(patch(json) == Right(Json.Object("b" -> Json.Number("3"))))
      }
    ),
    suite("number delta variants")(
      test("positive delta") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(5))))
        assertTrue(patch(Json.Number("10")) == Right(Json.Number("15")))
      },
      test("negative delta") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(-2))))
        assertTrue(patch(Json.Number("3")) == Right(Json.Number("1")))
      },
      test("zero delta") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(0))))
        assertTrue(patch(Json.Number("3")) == Right(Json.Number("3")))
      },
      test("decimal delta") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal("0.5"))))
        assertTrue(patch(Json.Number("1.25")) == Right(Json.Number("1.75")))
      }
    ),
    suite("modes")(
      test("Strict fails on invalid array delete") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Delete(2, 1))))
        val json  = Json.Array(Json.Number("1"))
        assertTrue(patch(json, JsonPatchMode.Strict).isLeft)
      },
      test("Lenient skips invalid array delete") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Delete(2, 1))))
        val json  = Json.Array(Json.Number("1"))
        assertTrue(patch(json, JsonPatchMode.Lenient) == Right(json))
      },
      test("Clobber clamps insert index") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Insert(5, Vector(Json.Number("2"))))))
        val json  = Json.Array(Json.Number("1"))
        assertTrue(patch(json, JsonPatchMode.Clobber) == Right(Json.Array(Json.Number("1"), Json.Number("2"))))
      },
      test("Clobber overwrites existing object key") {
        val patch = JsonPatch.root(Op.ObjectEdit(Vector(ObjectOp.Add("a", Json.Number("2")))))
        val json  = Json.Object("a" -> Json.Number("1"))
        assertTrue(patch(json, JsonPatchMode.Clobber) == Right(Json.Object("a" -> Json.Number("2"))))
      }
    ),
    suite("dynamic patch conversion")(
      test("toDynamicPatch/fromDynamicPatch roundtrip") {
        val patch = JsonPatch(
          Vector(
            JsonPatchOp(DynamicOptic.root, Op.Set(Json.Object("a" -> Json.Number("1")))),
            JsonPatchOp(DynamicOptic.root.field("a"), Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(2)))),
            JsonPatchOp(
              DynamicOptic.root.field("arr"),
              Op.ArrayEdit(Vector(ArrayOp.Append(Vector(Json.String("x")))))
            ),
            JsonPatchOp(
              DynamicOptic.root.field("obj"),
              Op.ObjectEdit(Vector(ObjectOp.Add("k", Json.Boolean(true))))
            ),
            JsonPatchOp(
              DynamicOptic.root,
              Op.Nested(JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Append("!"))))))
            )
          )
        )
        assertTrue(JsonPatch.fromDynamicPatch(patch.toDynamicPatch) == Right(patch))
      }
    ),
    suite("edge cases")(
      test("empty arrays") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Append(Vector(Json.Number("1"))))))
        assertTrue(patch(Json.Array.empty) == Right(Json.Array(Json.Number("1"))))
      },
      test("empty objects") {
        val patch = JsonPatch.root(Op.ObjectEdit(Vector(ObjectOp.Add("a", Json.Number("1")))))
        assertTrue(patch(Json.Object.empty) == Right(Json.Object("a" -> Json.Number("1"))))
      },
      test("empty strings") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.StringEdit(Vector(StringOp.Append("x")))))
        assertTrue(patch(Json.String("")) == Right(Json.String("x")))
      },
      test("nested structures") {
        val patch = JsonPatch(
          Vector(
            JsonPatchOp(
              DynamicOptic.root.field("a").at(0),
              Op.Set(Json.Number("2"))
            )
          )
        )
        val json = Json.Object("a" -> Json.Array(Json.Number("1")))
        assertTrue(patch(json) == Right(Json.Object("a" -> Json.Array(Json.Number("2")))))
      }
    ),
    suite("error cases")(
      test("invalid path") {
        val patch = JsonPatch(Vector(JsonPatchOp(DynamicOptic.root.field("a"), Op.Set(Json.Number("1")))))
        assertTrue(patch(Json.Number("0")).isLeft)
      },
      test("type mismatch") {
        val patch = JsonPatch.root(Op.PrimitiveDelta(PrimitiveOp.NumberDelta(BigDecimal(1))))
        assertTrue(patch(Json.String("a")).isLeft)
      },
      test("out of bounds index") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Modify(3, Op.Set(Json.Number("1"))))))
        assertTrue(patch(Json.Array(Json.Number("0"))).isLeft)
      }
    ),
    suite("mode law")(
      test("lenient subsumes strict") {
        val patch = JsonPatch.root(Op.ArrayEdit(Vector(ArrayOp.Delete(0, 1))))
        val json  = Json.Array(Json.Number("1"))
        assertTrue(
          patch(json, JsonPatchMode.Strict) == Right(Json.Array.empty) &&
            patch(json, JsonPatchMode.Lenient) == Right(Json.Array.empty)
        )
      }
    )
  )
}
