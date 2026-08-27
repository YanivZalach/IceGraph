import {
  PANEL_DIFF_AFTER_LABEL_CLASS,
  PANEL_DIFF_AFTER_VALUE_CLASS,
  PANEL_DIFF_BEFORE_LABEL_CLASS,
  PANEL_DIFF_BEFORE_VALUE_CLASS,
  PANEL_FIELD_LABEL_CLASS,
} from "../../../../components/PanelContent";
import type { FieldDiff } from "../../lib/details/diffMetadataFiles";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface RawDiffSectionProps {
  diffs: FieldDiff[];
}

const formatValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return value.toString();
  }
  return JSON.stringify(value, null, 2);
};

const RawDiffSection = ({ diffs }: RawDiffSectionProps) => {
  if (diffs.length === 0) {
    return null;
  }

  return (
    <details>
      <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
        Raw diff
      </summary>
      <div className="flex flex-col gap-4">
        {diffs.map((diff) => (
          <div key={diff.key} className="flex flex-col gap-1.5">
            <span className={PANEL_FIELD_LABEL_CLASS}>
              {diff.key.replace(/_/g, " ")}
            </span>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <div className={PANEL_DIFF_BEFORE_LABEL_CLASS}>Before</div>
                <pre className={PANEL_DIFF_BEFORE_VALUE_CLASS}>
                  {formatValue(diff.before)}
                </pre>
              </div>
              <div>
                <div className={PANEL_DIFF_AFTER_LABEL_CLASS}>After</div>
                <pre className={PANEL_DIFF_AFTER_VALUE_CLASS}>
                  {formatValue(diff.after)}
                </pre>
              </div>
            </div>
          </div>
        ))}
      </div>
    </details>
  );
};

export default RawDiffSection;
