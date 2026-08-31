import { z } from "zod";

export const icebergStructFieldSchema = z.object({
  id: z.number(),
  name: z.string(),
  required: z.boolean(),
  doc: z.string().nullish(),
  get type() {
    return icebergTypeSchema;
  },
});

const icebergStructTypeSchema = z.object({
  type: z.literal("struct"),
  fields: z.array(icebergStructFieldSchema),
});

const icebergListTypeSchema = z.object({
  type: z.literal("list"),
  "element-id": z.number(),
  "element-required": z.boolean(),
  get element() {
    return icebergTypeSchema;
  },
});

const icebergMapTypeSchema = z.object({
  type: z.literal("map"),
  "key-id": z.number(),
  "value-id": z.number(),
  "value-required": z.boolean(),
  get key() {
    return icebergTypeSchema;
  },
  get value() {
    return icebergTypeSchema;
  },
});

export const icebergTypeSchema = z.union([
  z.string(),
  icebergStructTypeSchema,
  icebergListTypeSchema,
  icebergMapTypeSchema,
]);

export type IcebergStructField = z.infer<typeof icebergStructFieldSchema>;
export type IcebergType = z.infer<typeof icebergTypeSchema>;
