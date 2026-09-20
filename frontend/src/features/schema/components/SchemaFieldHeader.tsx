import {
  SPEC_HEAD_CLASS,
  SpecHeaderCell,
} from "../../specs/components/SpecFieldTable";

const SchemaFieldHeader = () => (
  <thead className={SPEC_HEAD_CLASS}>
    <tr>
      <SpecHeaderCell>Field ID</SpecHeaderCell>
      <SpecHeaderCell>Column</SpecHeaderCell>
      <SpecHeaderCell>Type</SpecHeaderCell>
      <SpecHeaderCell>Required</SpecHeaderCell>
    </tr>
  </thead>
);

export default SchemaFieldHeader;
