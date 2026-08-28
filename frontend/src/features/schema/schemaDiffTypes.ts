import type { IcebergSchemaField, IcebergType } from "./schemaModel";

export type SchemaDiffStatus =
  | "unchanged"
  | "added"
  | "removed"
  | "changed"
  | "moved"
  | "descendant-changed";

interface TypeDiffBase {
  status: SchemaDiffStatus;
}

export interface PrimitiveTypeDiff extends TypeDiffBase {
  kind: "primitive";
  before: string | null;
  after: string | null;
}

export interface StructTypeDiff extends TypeDiffBase {
  kind: "struct";
  fields: SchemaFieldDiff[];
}

export interface ListTypeDiff extends TypeDiffBase {
  kind: "list";
  beforeElementId: string | null;
  afterElementId: string | null;
  beforeIsElementRequired: boolean | null;
  afterIsElementRequired: boolean | null;
  element: SchemaTypeDiff;
}

export interface MapTypeDiff extends TypeDiffBase {
  kind: "map";
  beforeKeyId: string | null;
  afterKeyId: string | null;
  key: SchemaTypeDiff;
  beforeValueId: string | null;
  afterValueId: string | null;
  beforeIsValueRequired: boolean | null;
  afterIsValueRequired: boolean | null;
  value: SchemaTypeDiff;
}

export interface UnknownTypeDiff extends TypeDiffBase {
  kind: "unknown";
  before: unknown;
  after: unknown;
}

export interface ReplacementTypeDiff extends TypeDiffBase {
  kind: "replacement";
  before: IcebergType;
  after: IcebergType;
}

export type SchemaTypeDiff =
  | PrimitiveTypeDiff
  | StructTypeDiff
  | ListTypeDiff
  | MapTypeDiff
  | UnknownTypeDiff
  | ReplacementTypeDiff;

export interface SchemaFieldDiff {
  status: SchemaDiffStatus;
  movement: "from" | "to" | null;
  before: IcebergSchemaField | null;
  after: IcebergSchemaField | null;
  isNameChanged: boolean;
  isRequiredChanged: boolean;
  type: SchemaTypeDiff;
}

export interface SchemaDiff {
  fields: SchemaFieldDiff[];
}
