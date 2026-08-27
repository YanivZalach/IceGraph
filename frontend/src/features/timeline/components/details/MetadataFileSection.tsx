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
      <table className="w-full">
        <thead>
          <tr>
            {data.stats.map((stat) => (
              <th
                key={stat.label}
                className={cn(
                  UI_FIELD_LABEL_CLASS,
                  "pb-1 text-center align-bottom normal-case break-words",
                )}
              >
                {stat.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <tr>
            {data.stats.map((stat) => (
              <td key={stat.label} className="text-center">
                <span className="inline-block rounded bg-canvas px-2 py-0.5 font-mono text-xs text-slate-200">
                  {stat.value === "" ? "—" : stat.value}
                </span>
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  </details>
);

export default MetadataFileSection;
