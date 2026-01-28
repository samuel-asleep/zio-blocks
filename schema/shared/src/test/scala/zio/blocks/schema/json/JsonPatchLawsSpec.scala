package zio.blocks.schema.json

import zio.blocks.chunk.Chunk
import zio.test._

object JsonPatchLawsSpec extends SchemaBaseSpec {

  private val genJson: Gen[Any, Json] = genJsonWithDepth(2)

  private def genJsonWithDepth(maxDepth: Int): Gen[Any, Json] = {
    val genLeaf = Gen.oneOf(
      Gen.const(Json.Null),
      Gen.boolean.map(Json.Boolean(_)),
      Gen.bigDecimal(BigDecimal(-1000), BigDecimal(1000)).map(bd => Json.Number(bd.toString)),
      Gen.alphaNumericStringBounded(0, 10).map(Json.String(_))
    )

    if (maxDepth <= 0) genLeaf
    else {
      val genArray = Gen
        .listOfBounded(0, 3)(genJsonWithDepth(maxDepth - 1))
        .map(values => new Json.Array(Chunk.from(values)))

      val genObject = Gen
        .listOfBounded(0, 3) {
          for {
            key   <- Gen.alphaNumericStringBounded(1, 8)
            value <- genJsonWithDepth(maxDepth - 1)
          } yield key -> value
        }
        .map(_.distinctBy(_._1))
        .map(fields => new Json.Object(Chunk.from(fields)))

      Gen.oneOf(genLeaf, genArray, genObject)
    }
  }

  def spec: Spec[TestEnvironment, Any] = suite("JsonPatchLawsSpec")(
    suite("Monoid laws")(
      test("left identity") {
        check(genJson, genJson) { (oldJson, newJson) =>
          val patch  = JsonPatch.diff(oldJson, newJson)
          val empty  = JsonPatch.empty
          val result = (empty ++ patch)(oldJson, JsonPatchMode.Strict)
          assertTrue(result == patch(oldJson, JsonPatchMode.Strict))
        }
      },
      test("right identity") {
        check(genJson, genJson) { (oldJson, newJson) =>
          val patch  = JsonPatch.diff(oldJson, newJson)
          val empty  = JsonPatch.empty
          val result = (patch ++ empty)(oldJson, JsonPatchMode.Strict)
          assertTrue(result == patch(oldJson, JsonPatchMode.Strict))
        }
      },
      test("associativity") {
        check(genJson, genJson, genJson, genJson) { (v0, v1, v2, v3) =>
          val p1 = JsonPatch.diff(v0, v1)
          val p2 = JsonPatch.diff(v1, v2)
          val p3 = JsonPatch.diff(v2, v3)
          val left  = (p1 ++ p2) ++ p3
          val right = p1 ++ (p2 ++ p3)
          assertTrue(left(v0, JsonPatchMode.Strict) == right(v0, JsonPatchMode.Strict))
        }
      }
    ),
    suite("diff/apply laws")(
      test("roundtrip") {
        check(genJson, genJson) { (oldJson, newJson) =>
          val patch = JsonPatch.diff(oldJson, newJson)
          assertTrue(patch(oldJson, JsonPatchMode.Strict) == Right(newJson))
        }
      },
      test("identity diff") {
        check(genJson) { json =>
          assertTrue(JsonPatch.diff(json, json).isEmpty)
        }
      },
      test("diff composition") {
        check(genJson, genJson, genJson) { (a, b, c) =>
          val patch = JsonPatch.diff(a, b) ++ JsonPatch.diff(b, c)
          assertTrue(patch(a, JsonPatchMode.Strict) == Right(c))
        }
      }
    ),
    suite("PatchMode law")(
      test("lenient subsumes strict") {
        check(genJson, genJson) { (oldJson, newJson) =>
          val patch  = JsonPatch.diff(oldJson, newJson)
          val strict = patch(oldJson, JsonPatchMode.Strict)
          strict match {
            case Right(result) =>
              assertTrue(patch(oldJson, JsonPatchMode.Lenient) == Right(result))
            case Left(_) =>
              assertTrue(true)
          }
        }
      }
    )
  )
}
