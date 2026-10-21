"""Arrow bridge for the native adapter; no dependency on a Python dbt adapter."""
import contextlib
import json
import sys

import adbc_driver_manager.dbapi as dbapi
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv


def arrow_result(result):
    if isinstance(result, (pa.Table, pa.RecordBatchReader)):
        return result
    if isinstance(result, pa.RecordBatch):
        return pa.Table.from_batches([result])
    library = type(result).__module__.split('.')[0]
    if library == 'pandas':
        return pa.Table.from_pandas(result, preserve_index=False)
    if library == 'polars':
        return (result.collect() if hasattr(result, 'collect') else result).to_arrow()
    if hasattr(result, '__arrow_c_stream__'):
        return pa.RecordBatchReader.from_stream(result)
    raise TypeError('model() must return an Arrow table/reader, pandas DataFrame, or polars DataFrame')


class Session:
    def __init__(self, connection):
        self.connection = connection

    def sql(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetch_arrow_table()


def run_model(connection, parameters):
    namespace = {'__name__': '__dbt_model__'}
    exec(compile(parameters['code'], 'dbt_model.py', 'exec'), namespace)
    session = Session(connection)
    result = arrow_result(namespace['model'](namespace['dbtObj'](lambda name: session.sql('select * from ' + name)), session))
    target = namespace['dbt_redis_adbc_target']
    with connection.cursor() as cursor:
        return cursor.adbc_ingest(target['identifier'], result, mode='create', db_schema_name=target['schema'])


def load_csv(connection, parameters):
    types = parameters['column_types']
    with connection.cursor() as cursor:
        cursor.execute(parameters['probe_sql'])
        schema = cursor.fetch_arrow_table().schema
    reader = csv.open_csv(parameters['path'], convert_options=csv.ConvertOptions(
        column_types={field.name: field.type for field in schema}, include_columns=list(types),
        timestamp_parsers=[parameters['timestamp_format']], strings_can_be_null=True))
    stride, limit = parameters['sample_every'], parameters['limit']
    row_name = parameters['row_number_column']
    output_schema = pa.schema([pa.field(row_name, pa.int64()), *schema]) if row_name else schema

    def batches():
        seen = emitted = 0
        for batch in reader:
            positions = pa.array(range((-seen) % stride, batch.num_rows, stride), pa.int64())
            selected = batch.take(positions)
            if row_name:
                selected = pa.RecordBatch.from_arrays([pc.add(positions, pa.scalar(seen + 1)), *selected.columns], schema=output_schema)
            seen += batch.num_rows
            if limit is not None:
                selected = selected.slice(0, max(0, limit - emitted))
            emitted += selected.num_rows
            if selected.num_rows:
                yield selected
            if limit is not None and emitted >= limit:
                break

    # Validate the sampled input before replacing an existing relation.
    table = pa.Table.from_batches(batches(), schema=output_schema)
    with connection.cursor() as cursor:
        cursor.execute(parameters['create_schema_sql'])
        cursor.execute(parameters['drop_table_sql'])
        return cursor.adbc_ingest(parameters['identifier'], table, mode='create', db_schema_name=parameters['schema'])


def main():
    request = json.load(sys.stdin)
    with contextlib.redirect_stdout(sys.stderr):
        with dbapi.connect(driver=request['driver'], entrypoint='AdbcDriverInit', db_kwargs=request['options'], autocommit=True) as connection:
            if request['operation'] == 'model':
                rows = run_model(connection, request['parameters'])
            elif request['operation'] == 'csv':
                rows = load_csv(connection, request['parameters'])
            else:
                raise ValueError('Unknown Arrow runtime operation')
    json.dump(rows, sys.stdout)


if __name__ == '__main__':
    main()
