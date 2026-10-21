use crate::connection::ConnectionGuard;
use crate::errors::{AdapterError, AdapterErrorKind, adbc_error_to_adapter_error};
use crate::metadata::CatalogAndSchema;
use crate::record_batch::StructArrayExt;
use crate::relation::Relation;
use crate::{AdapterEngine, AdapterResult};
use arrow_array::{Array, ListArray, RecordBatch, StringArray, StructArray};
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{ResolvedQuoting, normalize_quote};
use dbt_schemas::schemas::relations::base::BaseRelation;
use std::sync::Arc;

pub(crate) fn list_relations_via_adbc(
    engine: &dyn AdapterEngine,
    conn: &mut ConnectionGuard<'_>,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // The driver expects unquoted values, so regardless of the adapter's config
    // we need to strip quotes.
    let (catalog, _) = normalize_quote(false, engine.adapter_type(), &db_schema.rendered_catalog);
    let (schema, _) = normalize_quote(false, engine.adapter_type(), &db_schema.rendered_schema);

    let reader = conn
        .get_objects(
            adbc_core::options::ObjectDepth::Tables,
            Some(&catalog),
            Some(&schema),
            None,
            None,
            None,
        )
        .map_err(adbc_error_to_adapter_error)?;

    let arrow_schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| AdapterError::new(AdapterErrorKind::Driver, e.to_string()))?;
    let batch = arrow::compute::concat_batches(&arrow_schema, &batches)
        .map_err(|e| AdapterError::new(AdapterErrorKind::Driver, e.to_string()))?;

    relations_from_get_objects(
        &batch,
        engine.adapter_type(),
        engine.quoting(),
        &catalog,
        &schema,
    )
}

fn relations_from_get_objects(
    batch: &RecordBatch,
    adapter_type: AdapterType,
    quoting: ResolvedQuoting,
    catalog: &str,
    schema: &str,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // The schema names are nested in the result of the get object call.
    // The batch has the following shape at the top level:
    // - catalog_name: utf8
    // - catalog_db_schemas: list[struct]
    //
    // Each row of the column `catalog_db_schemas` is a list of elements
    // with the shape:
    //   - db_schema_name: utf8
    //   - db_schema_tables: list
    let catalog_db_schemas = batch
        .column_by_name("catalog_db_schemas")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Missing or invalid 'catalog_db_schemas' column",
            )
        })?;

    let schemas_struct = catalog_db_schemas
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Missing or invalid 'catalog_db_schemas' values",
            )
        })?;

    // Each row of the column `db_schema_tables` is a list of elements
    // with the shape:
    //   - table_name: utf8
    //   - table_type: utf8
    //   - table_columns: list
    //   - table_constraints: list
    let db_schema_tables = schemas_struct.column_as::<ListArray>("db_schema_tables")?;

    let schema_names = schemas_struct.column_as::<StringArray>("db_schema_name")?;
    let mut result = Vec::new();
    for row in 0..schemas_struct.len() {
        if schema_names.is_null(row)
            || schema_names.value(row) != schema
            || db_schema_tables.is_null(row)
        {
            continue;
        }
        let tables = db_schema_tables.value(row);
        let tables = tables
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::UnexpectedResult,
                    "Invalid db_schema_tables values",
                )
            })?;
        let names = tables.column_as::<StringArray>("table_name")?;
        let types = tables.column_as::<StringArray>("table_type")?;
        for row in 0..tables.len() {
            let relation_type = match adapter_type {
                AdapterType::RedisAdbc => match types.value(row).to_ascii_uppercase().as_str() {
                    "TABLE" => RelationType::Table,
                    "VIEW" => RelationType::View,
                    other => {
                        return Err(AdapterError::new(
                            AdapterErrorKind::UnexpectedResult,
                            format!("Unsupported Redis relation type: {other}"),
                        ));
                    }
                },
                _ => RelationType::from_adapter_type(adapter_type, types.value(row)),
            };
            result.push(Arc::new(
                Relation::new(
                    adapter_type,
                    catalog.to_owned(),
                    schema.to_owned(),
                    names.value(row).to_owned(),
                )
                .with_relation_type(relation_type)
                .with_quoting(quoting),
            ) as Arc<dyn BaseRelation>);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::OffsetBuffer;
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};
    use dbt_schemas::schemas::relations::base::Policy;

    #[test]
    fn metadata_keeps_exact_schema_and_adapter_identity() {
        // ADBC schema filters use SQL LIKE: analytics_one can also match
        // analyticsXone. Only relations in the requested schema may be cached.
        let tables = StructArray::from(vec![
            (
                Arc::new(Field::new("table_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["Trips", "other_schema_table"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("table_type", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["TABLE", "VIEW"])) as ArrayRef,
            ),
        ]);
        let tables = ListArray::new(
            Arc::new(Field::new("item", tables.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 1, 2].into()),
            Arc::new(tables),
            None,
        );
        let schemas = StructArray::from(vec![
            (
                Arc::new(Field::new("db_schema_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["analytics_one", "analyticsXone"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "db_schema_tables",
                    tables.data_type().clone(),
                    true,
                )),
                Arc::new(tables) as ArrayRef,
            ),
        ]);
        let catalogs = ListArray::new(
            Arc::new(Field::new("item", schemas.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 2].into()),
            Arc::new(schemas),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "catalog_db_schemas",
                catalogs.data_type().clone(),
                true,
            )])),
            vec![Arc::new(catalogs)],
        )
        .unwrap();
        for adapter_type in [AdapterType::RedisAdbc, AdapterType::Bigquery] {
            let relations = relations_from_get_objects(
                &batch,
                adapter_type,
                Policy::trues(),
                "redis",
                "analytics_one",
            )
            .unwrap();
            assert_eq!(relations.len(), 1);
            assert_eq!(relations[0].adapter_type(), adapter_type);
            assert_eq!(relations[0].identifier(), Some("Trips"));
            assert_eq!(relations[0].relation_type(), Some(RelationType::Table));
        }
    }
}
