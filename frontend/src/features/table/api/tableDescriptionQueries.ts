import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { ApiError, fetchFromApi } from "../../../shared/lib/api";

const NOT_ICEBERG_TABLE_ERROR_PATTERN =
  /^Table '.*' is not an Iceberg table\.$/s;

const UNREACHABLE_METADATA_FILE_ERROR_PATTERN =
  /^Table '.*' is unreachable: its current metadata file can't be read: (\S+)$/s;

const tableDescriptionRowsSchema = z.array(
  z.object({
    name: z.string(),
    value: z.string(),
    comment: z.string(),
  }),
);

const tableDescriptionSchema = z.object({
  spark_schema: z.unknown(),
  spark_partitions: tableDescriptionRowsSchema,
  sections: z.array(
    z.object({ title: z.string(), rows: tableDescriptionRowsSchema }),
  ),
  properties: z.record(z.string(), z.string()).nullable(),
});

export type TableDescriptionRow = z.infer<
  typeof tableDescriptionRowsSchema
>[number];

export const isNotIcebergTableError = (error: unknown): error is ApiError =>
  error instanceof ApiError &&
  NOT_ICEBERG_TABLE_ERROR_PATTERN.test(error.message);

export const getUnreachableMetadataFile = (error: unknown): string | null =>
  error instanceof ApiError
    ? (UNREACHABLE_METADATA_FILE_ERROR_PATTERN.exec(error.message)?.[1] ?? null)
    : null;

export const tableDescriptionQueryOptions = (tableName: string) =>
  queryOptions({
    queryKey: ["table-description", tableName] as const,
    queryFn: ({ signal }) =>
      fetchFromApi(
        `/table-description?${new URLSearchParams({ table_name: tableName }).toString()}`,
        tableDescriptionSchema,
        { signal },
      ),
    enabled: tableName !== "",
    retry: 1,
    staleTime: 60 * 1000,
  });
