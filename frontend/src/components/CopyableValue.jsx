import CopyIconButton from "./CopyIconButton";
import {
  UI_COPYABLE_VALUE_CLASS,
  UI_EMPTY_PLACEHOLDER_CLASS,
} from "../uiTypography";

export default function CopyableValue({
  value,
  mono = false,
  className = "",
  boxClassName = "bg-canvas rounded-lg",
}) {
  const hasValue = value != null && value !== "";
  const textToCopy = hasValue ? String(value) : "";

  return (
    <div className={`relative ${className}`}>
      {hasValue && (
        <CopyIconButton
          text={textToCopy}
          className="absolute top-1.5 right-1.5 z-10"
        />
      )}
      <span
        className={`${UI_COPYABLE_VALUE_CLASS} ${boxClassName} ${mono ? "font-mono" : ""}`}
      >
        {hasValue ? (
          value
        ) : (
          <span className={UI_EMPTY_PLACEHOLDER_CLASS}>—</span>
        )}
      </span>
    </div>
  );
}
