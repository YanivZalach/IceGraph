import type { CSSProperties } from "react";

export const PANEL_SUBTITLE_CLASS =
  "text-sm font-mono text-slate-400 mt-1 break-words";

const PRESERVED_SUBTITLE_STYLE: CSSProperties = {
  direction: "rtl",
  textAlign: "left",
  textOverflow: "ellipsis",
};
const LEFT_TO_RIGHT_EMBEDDING = "\u202A";
const POP_DIRECTIONAL_FORMATTING = "\u202C";

interface PanelSubtitleProps {
  preserveEnd: boolean;
  subtitle: string;
}

const renderBreakablePath = (path: string) => {
  const segments = path.split("/");
  return segments.map((segment, index) => (
    <span key={index}>
      {segment}
      {index < segments.length - 1 ? (
        <>
          /<wbr />
        </>
      ) : null}
    </span>
  ));
};

const PanelSubtitle = ({ preserveEnd, subtitle }: PanelSubtitleProps) =>
  preserveEnd ? (
    <div
      className={`${PANEL_SUBTITLE_CLASS} overflow-hidden text-ellipsis whitespace-nowrap text-left`}
      style={PRESERVED_SUBTITLE_STYLE}
      title={subtitle}
    >
      {LEFT_TO_RIGHT_EMBEDDING + subtitle + POP_DIRECTIONAL_FORMATTING}
    </div>
  ) : (
    <div className={PANEL_SUBTITLE_CLASS}>{renderBreakablePath(subtitle)}</div>
  );

export default PanelSubtitle;
