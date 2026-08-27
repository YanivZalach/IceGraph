import CopyIconButton from "../../../../components/CopyIconButton";
import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import type { MetadataFileData } from "../../lib/details/buildEventDetails";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface MetadataFileSectionProps {
  data: MetadataFileData;
}

const MetadataFileSection = ({ data }: MetadataFileSectionProps) => (
  <details>
    <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
      Metadata file
    </summary>
    <div className="flex flex-col gap-3">
      <div>
        <span className={cn(UI_FIELD_LABEL_CLASS, "normal-case")}>Path</span>
        <span className="mt-1 flex items-center gap-1.5 rounded bg-canvas px-2 py-0.5 font-mono text-xs break-all text-slate-200">
          {data.path}
          <CopyIconButton text={data.path} />
        </span>
      </div>
      <div className="grid grid-cols-2 gap-x-6">
        {data.stats.map((stat) => (
          <div
            key={stat.label}
            className="flex items-center justify-between gap-x-3 border-b border-edge/50 py-1.5 nth-last-[-n+2]:border-0"
          >
            <span className={cn(UI_FIELD_LABEL_CLASS, "normal-case")}>
              {stat.label}
            </span>
            <span className="rounded bg-canvas px-2 py-0.5 font-mono text-xs text-slate-200">
              {stat.value === "" ? "—" : stat.value}
            </span>
          </div>
        ))}
      </div>
    </div>
  </details>
);

export default MetadataFileSection;
