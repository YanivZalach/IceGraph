import { highlightJson } from "../../../utils/jsonHighlight";
import CopyIconButton from "../../../components/CopyIconButton";

interface MetadataJsonProps {
  text: string;
  label: string;
}

const MetadataJson = ({ text, label }: MetadataJsonProps) => (
  <div className="relative">
    <CopyIconButton
      text={text}
      title={`Copy ${label}`}
      className="absolute top-1.5 right-1.5 z-10"
    />
    <pre
      data-metadata-scroll
      tabIndex={0}
      aria-label={label}
      className="max-h-96 overflow-auto rounded-lg border border-edge bg-canvas p-4 font-mono text-xs leading-relaxed focus-visible:outline-2 focus-visible:outline-accent"
    >
      {highlightJson(text)}
    </pre>
  </div>
);
export default MetadataJson;
