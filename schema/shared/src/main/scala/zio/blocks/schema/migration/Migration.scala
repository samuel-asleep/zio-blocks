package zio.blocks.schema.migration

import zio.blocks.schema._

/**
 * A type-safe migration from values of type `A` to values of type `B`, backed
 * by a [[DynamicMigration]] operating at the [[DynamicValue]] level.
 *
 * @param dynamicMigration the underlying untyped migration
 * @param sourceSchema the schema for the source type `A`
 * @param targetSchema the schema for the target type `B`
 * @tparam A source type
 * @tparam B target type
 */
final case class Migration[A, B](
  dynamicMigration: DynamicMigration,
  sourceSchema: Schema[A],
  targetSchema: Schema[B]
) {

  /**
   * Applies this migration to a value of type `A`, converting it to type `B`.
   *
   * The value is first converted to a [[DynamicValue]] using `sourceSchema`,
   * the migration is then applied, and the result is decoded using `targetSchema`.
   *
   * @param value the source value to migrate
   * @return the migrated value or a [[SchemaError]]
   */
  def apply(value: A): Either[SchemaError, B] =
    dynamicMigration(sourceSchema.toDynamicValue(value))
      .flatMap(targetSchema.fromDynamicValue)

  /**
   * Composes this migration with `that`, yielding a migration from `A` to `C`.
   *
   * @tparam C the final target type
   */
  def ++[C](that: Migration[B, C]): Migration[A, C] =
    Migration(dynamicMigration ++ that.dynamicMigration, sourceSchema, that.targetSchema)

  /** Alias for [[++]]. */
  def andThen[C](that: Migration[B, C]): Migration[A, C] = this ++ that

  /** Returns a migration that undoes this one, converting `B` back to `A`. */
  def reverse: Migration[B, A] =
    Migration(dynamicMigration.reverse, targetSchema, sourceSchema)
}

object Migration {

  /**
   * Creates an identity migration for type `A` that leaves every value unchanged.
   *
   * @param schema the schema for type `A`
   */
  def identity[A](implicit schema: Schema[A]): Migration[A, A] =
    Migration(DynamicMigration.identity, schema, schema)

  /**
   * Returns a [[MigrationBuilder]] for constructing a migration from `A` to `B`.
   *
   * {{{
   * val m = Migration.builder[PersonV1, PersonV2]
   *   .rename(DynamicOptic.root.field("firstName"), "first_name")
   *   .addField(DynamicOptic.root.field("active"), DynamicValue.Primitive(PrimitiveValue.Boolean(true)))
   *   .build
   * }}}
   *
   * @param sourceSchema the schema for the source type `A`
   * @param targetSchema the schema for the target type `B`
   */
  def builder[A, B](implicit sourceSchema: Schema[A], targetSchema: Schema[B]): MigrationBuilder[A, B] =
    new MigrationBuilder[A, B](Vector.empty, sourceSchema, targetSchema)
}
