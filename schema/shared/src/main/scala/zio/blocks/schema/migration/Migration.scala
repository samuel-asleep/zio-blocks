package zio.blocks.schema.migration

import zio.blocks.schema._

/**
 * A type-safe migration from values of type `A` to values of type `B`, backed
 * by a [[DynamicMigration]] operating at the [[DynamicValue]] level.
 *
 * {{{
 * @schema case class PersonV1(name: String)
 * @schema case class PersonV2(fullName: String, age: Int)
 *
 * val migration: Migration[PersonV1, PersonV2] =
 *   Migration.builder[PersonV1, PersonV2]
 *     .rename(DynamicOptic.root.field("name"), "fullName")
 *     .addField(DynamicOptic.root.field("age"), DynamicValue.Primitive(PrimitiveValue.Int(0)))
 *     .build
 *
 * migration(PersonV1("Alice")) match {
 *   case Right(PersonV2(fullName, age)) => // PersonV2("Alice", 0)
 *   case Left(err)                      => // SchemaError
 * }
 *
 * // Compose migrations  A -> B -> C
 * val chained: Migration[PersonV1, PersonV3] = migration ++ migration2
 *
 * // Reverse  B -> A
 * val reverse: Migration[PersonV2, PersonV1] = migration.reverse
 * }}}
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
   *   .rename(DynamicOptic.root.field("firstName"), "fullName")
   *   .addField(
   *     DynamicOptic.root.field("active"),
   *     DynamicValue.Primitive(PrimitiveValue.Boolean(true))
   *   )
   *   .build
   *
   * m(PersonV1("Alice")) match {
   *   case Right(v2) => // PersonV2("Alice", active = true)
   *   case Left(err) => // SchemaError
   * }
   * }}}
   *
   * @param sourceSchema the schema for the source type `A`
   * @param targetSchema the schema for the target type `B`
   */
  def builder[A, B](implicit sourceSchema: Schema[A], targetSchema: Schema[B]): MigrationBuilder[A, B] =
    new MigrationBuilder[A, B](Vector.empty, sourceSchema, targetSchema)
}
