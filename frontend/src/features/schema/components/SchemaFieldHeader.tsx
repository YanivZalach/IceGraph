import {
  SPEC_HEAD_CLASS,
  SpecHeaderCell,
} from "../../specs/components/SpecFieldTable";

interface SchemaFieldHeaderProps {
  showIds?: boolean;
}

const SchemaFieldHeader = ({ showIds = true }: SchemaFieldHeaderProps) => (
  <thead className={SPEC_HEAD_CLASS}>
    <tr>
      {showIds && <SpecHeaderCell>Field ID</SpecHeaderCell>}
      <SpecHeaderCell>Column</SpecHeaderCell>
      <SpecHeaderCell>Type</SpecHeaderCell>
      <SpecHeaderCell>Required</SpecHeaderCell>
    </tr>
  </thead>
);

export default SchemaFieldHeader;
