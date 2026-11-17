import CopyIconButton from "../../../../components/CopyIconButton";
import { PANEL_FIELD_LABEL_CLASS } from "../../../../components/PanelContent";

interface DetailValueRowProps {
  label: string;
  value: string;
}

/** Like the shared panel row, but long values wrap so the whole value is visible at once. */
const DetailValueRow = ({ label, value }: DetailValueRowProps) => (
  <div>
    <span className={`mb-1 block ${PANEL_FIELD_LABEL_CLASS}`}>{label}</span>
    <div className="relative">
      {value !== "" && (
        <CopyIconButton text={value} className="absolute top-1 right-2 z-10" />
      )}
      <span className="block rounded-lg bg-canvas py-2 pr-9 pl-3 font-mono text-xs break-all whitespace-pre-wrap text-slate-200">
        {value === "" ? "-" : value}
      </span>
    </div>
  </div>
);

export default DetailValueRow;
