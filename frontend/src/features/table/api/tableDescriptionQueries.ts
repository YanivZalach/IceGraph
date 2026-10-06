import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { ApiError, fetchFromApi } from "../../../shared/lib/api";

const NOT_ICEBERG_TABLE_ERROR_CODE = "not_iceberg_table";

const notIcebergTableErrorBodySchema = z.object({
  error_code: z.literal(NOT_ICEBERG_TABLE_ERROR_CODE),
});

const UNREACHABLE_METADATA_FILE_ERROR_CODE = "unreachable_metadata_file";

const unreachableMetadataFileErrorBodySchema = z.object({
  error_code: z.literal(UNREACHABLE_METADATA_FILE_ERROR_CODE),
  metadata_file: z.string(),
});

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
  notIcebergTableErrorBodySchema.safeParse(error.body).success;

export const getUnreachableMetadataFile = (error: unknown): string | null => {
  if (!(error instanceof ApiError)) return null;
  const parsedErrorBody = unreachableMetadataFileErrorBodySchema.safeParse(
    error.body,
  );
  return parsedErrorBody.success ? parsedErrorBody.data.metadata_file : null;
};

export const tableDescriptionQueryOptions = (tableName: string) =>
  queryOptions({
    queryKey: ["table-description", tableName] as const,
    queryFn: ({ signal }) =>
      fetchFromApi(
        `/table-description/${encodeURIComponent(tableName)}`,
        tableDescriptionSchema,
        { signal },
      ),
    enabled: tableName !== "",
    retry: 1,
    staleTime: 60 * 1000,
  });
