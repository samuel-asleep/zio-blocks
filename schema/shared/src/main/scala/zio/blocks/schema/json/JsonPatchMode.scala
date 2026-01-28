package zio.blocks.schema.json

import zio.blocks.schema.patch.PatchMode

/**
 * Determines how a [[JsonPatch]] handles failures when applying operations.
 *
 * JsonPatch modes mirror [[zio.blocks.schema.patch.PatchMode]] for consistency:
 *  - Strict: fail fast on the first error
 *  - Lenient: skip failing operations
 *  - Clobber: overwrite on conflicts where possible
 */
sealed trait JsonPatchMode { self =>
  private[json] def toPatchMode: PatchMode = self match {
    case JsonPatchMode.Strict  => PatchMode.Strict
    case JsonPatchMode.Lenient => PatchMode.Lenient
    case JsonPatchMode.Clobber => PatchMode.Clobber
  }
}

object JsonPatchMode {

  /** Fail on the first precondition violation. */
  case object Strict extends JsonPatchMode

  /** Skip operations that fail preconditions. */
  case object Lenient extends JsonPatchMode

  /** Overwrite or clamp operations where possible to force success. */
  case object Clobber extends JsonPatchMode
}
