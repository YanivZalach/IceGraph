import { useState } from "react";

interface CopyIconButtonProps {
  text: string;
  className?: string;
  title?: string;
}

const CopyIconButton = ({
  text,
  className = "",
  title = "Copy value",
}: CopyIconButtonProps) => {
  const [copied, setCopied] = useState(false);

  if (!text) return null;

  const handleCopy = () => {
    void navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => {
      setCopied(false);
    }, 2000);
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      onMouseDown={(event) => {
        event.preventDefault();
      }}
      title={copied ? "Copied!" : title}
      aria-label={copied ? "Copied!" : title}
      className={`p-1 rounded border border-edge bg-surface/90 text-slate-500 hover:text-slate-300 hover:border-slate-500 transition-colors cursor-pointer ${className}`}
    >
      {copied ? (
        <svg
          className="size-3.5 text-green-400"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="2.5"
        >
          <path
            d="M3 8l3.5 3.5L13 4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      ) : (
        <svg
          className="size-3.5"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
        >
          <rect x="5" y="5" width="8" height="9" rx="1.5" />
          <path
            d="M11 5V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h1"
            strokeLinecap="round"
          />
        </svg>
      )}
    </button>
  );
};

export default CopyIconButton;
