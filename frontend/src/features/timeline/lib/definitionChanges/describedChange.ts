import type { ImpactSegment } from "../impactSegment";

export interface DescribedChange {
  impact: string;
  detail: string;
  impactSegment?: ImpactSegment;
}

export const sameTextChange = (text: string): DescribedChange => ({
  impact: text,
  detail: text,
});

export const formatId = (id: number | null): string =>
  id === null ? "?" : id.toString();
