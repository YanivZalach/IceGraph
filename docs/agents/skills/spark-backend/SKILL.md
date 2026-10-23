---
name: spark-backend
description: How to write Spark code in the IceGraph backend. Covers Spark Connect actions and driver round trips, when a collect is allowed, keeping the number of Spark actions independent of the number of files, where errors surface for try/except, reading Iceberg JSON and Avro files, and handling format-version differences. Use before writing, changing, or reviewing any PySpark code in backend/.
---

# Spark in the IceGraph backend

IceGraph talks to Spark through Spark Connect. The client only builds plans; work happens on the server
when the plan is analyzed or executed. Every server round trip costs latency, and every action costs a
Spark job.

## 1. Driver round trips need approval

These send a request to the server, and most run a job:

- Actions that bring data to the driver: `collect`, `first`, `head`, `take`, `toPandas`, `toLocalIterator`,
  `show`, `count`, `isEmpty`
- Analysis calls: `.schema`, `.columns`, `.dtypes`, `printSchema`, `explain`. They resolve the plan on the
  server and can open files to infer their schema
- Reads with schema inference, such as `spark.read.json(path)` or `spark.read.format("avro").load(path)`
  followed by an analysis call

Never add one without the user's permission. In the plan, state how many round trips the change adds or
removes compared with `master`.

## 2. The number of jobs must not depend on the number of files

Build one DataFrame per stage and run one action on it:

- Build a lazy DataFrame per file, combine them with `unionByName`, then collect once. See
  `extractors/data_files_extractor.py` and `collectors/collect_metadata.py`
- Never run an action inside a loop over snapshots, manifests, or files
- Per-file analysis calls, such as reading `df.schema` of each manifest to adapt to its fields, are
  allowed only when the file's schema decides the plan. They still count as round trips; say so

## 3. Laziness decides where errors appear

Defining a DataFrame does nothing. A missing file, a bad path, or an unknown column raises when the plan is
analyzed (`.schema`, `.columns`) or executed (an action), not where the DataFrame was defined.

- Put the `try` around the call that analyzes or executes, not only around the definition
- To isolate a bad file, the per-file `try` must include an analysis call on that file. Otherwise the error
  surfaces later, in the single collect, and fails the whole stage. See `Extractor._read_source` in
  `backend/extractors/extractor.py`, which calls `.schema` inside its `try`
- Keep the `try` as narrow as the failure it reports

## 4. Bring back only what Python uses

- Filter, reshape, and aggregate in Spark: `select`, `filter`, `map_filter`, `transform`, `get_json_object`,
  `collect_list`, window functions
- Select only the columns Python reads, then collect
- Never load a whole file into Python to pick fields from it
- Read each file once per request and reuse the result. Do not add a fallback that reads the same file
  again. See `collectors/collect_table_metadata.py`, which reads `metadata.json` once and returns the
  trimmed metadata, the current snapshot, and snapshot row lineage in one row

## 5. Reading JSON

Do not use `spark.read.json` on Iceberg JSON files: schema inference runs an extra job and the inferred
types depend on the file.

- Read with `spark.read.text(path, wholetext=True)` and parse with `F.from_json` and an explicit type
- For metadata with version-dependent fields, parse the top level as `MapType(StringType(), StringType())`.
  Objects and arrays stay raw JSON text; scalar strings lose their quotes, so list such keys explicitly
  (see `STRING_METADATA_FIELDS`)
- Extract nested values with `F.get_json_object(column, "$['hyphen-key']")`
- Do not use `prefersDecimal`; it turns numeric strings such as `"01234"` into numbers

## 6. Schemas and format versions

- Define schemas with `pyspark.sql.types` objects. Prefer a fixed read schema when the file layout is
  known, such as `MANIFEST_LIST_READ_SCHEMA`
- A field missing in older format versions becomes a typed null: `F.col(field)` if present, otherwise
  `F.lit(None).cast(data_type)`
- End each per-file plan with an explicit `select` of the output columns, so every file yields the same
  columns before the union and helper columns do not leak
- Skip work that does not apply. For example, the row-ID window runs only for manifests with row-ID
  assignments, so v1 and v2 tables pay nothing

## 7. Verifying a change

- Report the round trips the change adds or removes compared with `master`, and confirm none scale with
  the number of files
- Run the change against format version 1, 2, and 3 tables and compare the output with `master`
- Follow the Python rules in `AGENTS.md`
