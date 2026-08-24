import CopyIconButton from "../../../components/CopyIconButton";
import {
  UI_FIELD_LABEL_CLASS,
  UI_MONO_VALUE_CLASS,
} from "../../../uiTypography";

interface DetailRowProps {
  label: string;
  value: string;
  isCopyable?: boolean;
}

const DetailRow = ({ label, value, isCopyable = false }: DetailRowProps) => (
  <div className="flex flex-col gap-1">
    <span className={UI_FIELD_LABEL_CLASS}>{label}</span>
    <div className="flex items-start gap-2">
      <span className={UI_MONO_VALUE_CLASS}>{value === "" ? "—" : value}</span>
      {isCopyable && <CopyIconButton text={value} />}
    </div>
  </div>
);

export default DetailRow;
