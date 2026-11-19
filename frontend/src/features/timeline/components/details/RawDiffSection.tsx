import { describeFieldDiff } from "../../lib/details/describeFieldDiff";
import type { FieldDiff } from "../../lib/details/diffMetadataFiles";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";
import FieldDiffBlock from "./FieldDiffBlock";

interface RawDiffSectionProps {
  diffs: FieldDiff[];
}

const countText = (count: number, word: string): string =>
  `${count.toString()} ${word}${count === 1 ? "" : "s"}`;

const RawDiffSection = ({ diffs }: RawDiffSectionProps) => {
  if (diffs.length === 0) {
    return null;
  }

  return (
    <details>
      <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
        Raw metadata diff
        <span className="ml-2 text-sm font-normal tracking-normal text-slate-300 normal-case">
          {countText(diffs.length, "field")} changed
        </span>
      </summary>
      <div className="flex flex-col">
        {diffs.map((diff) => (
          <FieldDiffBlock key={diff.key} field={describeFieldDiff(diff)} />
        ))}
      </div>
    </details>
  );
};

export default RawDiffSection;
