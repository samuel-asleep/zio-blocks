package zio.blocks.schema.migration

import zio.blocks.schema._

/**
 * A fluent builder for constructing [[Migration]] instances by accumulating
 * [[MigrationAction]] steps.
 *
 * Obtain an instance via [[Migration.builder]].
 *
 * {{{
 * // Rename "name" -> "fullName", add "active" field with default false,
 * // and convert the "code" field from Int to String
 * val m = Migration.builder[PersonV1, PersonV2]
 *   .rename(DynamicOptic.root.field("name"), "fullName")
 *   .addField(
 *     DynamicOptic.root.field("active"),
 *     DynamicValue.Primitive(PrimitiveValue.Boolean(false))
 *   )
 *   .changeType(DynamicOptic.root.field("code"), TypeConverter.IntToString)
 *   .build
 *
 * m(PersonV1("Alice", 42)) match {
 *   case Right(v2) => // PersonV2("Alice", active = false, code = "42")
 *   case Left(err) => // SchemaError
 * }
 * }}}
 *
 * @tparam A
 *   the source type
 * @tparam B
 *   the target type
 */
final class MigrationBuilder[A, B] private[migration] (
  private val actions: Vector[MigrationAction],
  private val sourceSchema: Schema[A],
  private val targetSchema: Schema[B]
) {

  /**
   * Adds an [[MigrationAction.AddField]] step that inserts a field at `at` with
   * `default`.
   *
   * {{{
   * builder.addField(
   *   DynamicOptic.root.field("score"),
   *   DynamicValue.Primitive(PrimitiveValue.Int(0))
   * )
   * }}}
   *
   * @param at
   *   path to the new field (must end in a Field node)
   * @param default
   *   value to insert when the field is absent
   */
  def addField(at: DynamicOptic, default: DynamicValue): MigrationBuilder[A, B] =
    append(MigrationAction.AddField(at, default))

  /**
   * Adds a [[MigrationAction.DropField]] step that removes the field at `at`.
   *
   * @param at
   *   path to the field to remove
   * @param defaultForReverse
   *   value used when reversing the migration
   */
  def dropField(at: DynamicOptic, defaultForReverse: DynamicValue): MigrationBuilder[A, B] =
    append(MigrationAction.DropField(at, defaultForReverse))

  /**
   * Adds a [[MigrationAction.Rename]] step. The path `at` must end in a Field
   * node whose name is the original field name.
   *
   * {{{
   * // Rename top-level field "firstName" to "fullName"
   * builder.rename(DynamicOptic.root.field("firstName"), "fullName")
   *
   * // Rename nested field "address.zip" to "address.postalCode"
   * builder.rename(DynamicOptic.root.field("address").field("zip"), "postalCode")
   * }}}
   *
   * @param at
   *   path ending in the original Field node
   * @param to
   *   the new field name
   */
  def rename(at: DynamicOptic, to: String): MigrationBuilder[A, B] =
    append(MigrationAction.Rename(at, to))

  /**
   * Adds a [[MigrationAction.TransformValue]] step that applies `transform` to
   * the value at `at`.
   *
   * @param at
   *   path to the value to transform
   * @param transform
   *   the nested migration to apply
   */
  def transformValue(at: DynamicOptic, transform: DynamicMigration): MigrationBuilder[A, B] =
    append(MigrationAction.TransformValue(at, transform))

  /**
   * Adds a [[MigrationAction.Mandate]] step that makes an optional
   * (`Some`/`None`) field required. Fails at migration time if the field is
   * `None`.
   *
   * @param at
   *   path to the optional field
   * @param default
   *   default value used when reversing
   */
  def mandate(at: DynamicOptic, default: DynamicValue): MigrationBuilder[A, B] =
    append(MigrationAction.Mandate(at, default))

  /**
   * Adds an [[MigrationAction.Optionalize]] step that wraps the field value at
   * `at` in `Some`.
   *
   * @param at
   *   path to the field to optionalize
   */
  def optionalize(at: DynamicOptic): MigrationBuilder[A, B] =
    append(MigrationAction.Optionalize(at))

  /**
   * Adds a [[MigrationAction.ChangeType]] step that converts the primitive
   * value at `at` using `converter`.
   *
   * @param at
   *   path to the primitive field
   * @param converter
   *   the type converter to apply
   */
  def changeType(at: DynamicOptic, converter: TypeConverter): MigrationBuilder[A, B] =
    append(MigrationAction.ChangeType(at, converter))

  /**
   * Adds a [[MigrationAction.RenameCase]] step. The path `at` must end in a
   * Case node identifying the original case name.
   *
   * @param at
   *   path ending in the original Case node
   * @param to
   *   the new case name
   */
  def renameCase(at: DynamicOptic, to: String): MigrationBuilder[A, B] =
    append(MigrationAction.RenameCase(at, to))

  /**
   * Adds a [[MigrationAction.TransformCase]] step. The path `at` must end in a
   * Case node identifying which case to transform.
   *
   * @param at
   *   path ending in the Case node
   * @param caseActions
   *   actions to apply to the case value
   */
  def transformCase(at: DynamicOptic, caseActions: Vector[MigrationAction]): MigrationBuilder[A, B] =
    append(MigrationAction.TransformCase(at, caseActions))

  /**
   * Adds a [[MigrationAction.TransformElements]] step that applies `transform`
   * to every element of the sequence at `at`.
   *
   * @param at
   *   path to the sequence
   * @param transform
   *   the migration to apply to each element
   */
  def transformElements(at: DynamicOptic, transform: DynamicMigration): MigrationBuilder[A, B] =
    append(MigrationAction.TransformElements(at, transform))

  /**
   * Adds a [[MigrationAction.TransformKeys]] step that applies `transform` to
   * every key of the map at `at`.
   *
   * @param at
   *   path to the map
   * @param transform
   *   the migration to apply to each key
   */
  def transformKeys(at: DynamicOptic, transform: DynamicMigration): MigrationBuilder[A, B] =
    append(MigrationAction.TransformKeys(at, transform))

  /**
   * Adds a [[MigrationAction.TransformValues]] step that applies `transform` to
   * every value of the map at `at`.
   *
   * @param at
   *   path to the map
   * @param transform
   *   the migration to apply to each map value
   */
  def transformValues(at: DynamicOptic, transform: DynamicMigration): MigrationBuilder[A, B] =
    append(MigrationAction.TransformValues(at, transform))

  /** Builds the [[Migration]] from the accumulated actions. */
  def build: Migration[A, B] =
    Migration(DynamicMigration(actions), sourceSchema, targetSchema)

  private def append(action: MigrationAction): MigrationBuilder[A, B] =
    new MigrationBuilder[A, B](actions :+ action, sourceSchema, targetSchema)
}
