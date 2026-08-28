import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";
import { Children } from "react";
import { cn } from "../../../shared/lib/cn";

interface MarkdownContentProps {
  markdown: string;
  sectionId: string;
}

const MarkdownContent = ({ markdown, sectionId }: MarkdownContentProps) => {
  const isKeyboardShortcuts = sectionId === "keyboard-shortcuts";
  const isOverview = sectionId === "overview";
  const isIssuesPanel = sectionId === "issues-panel";

  const components: Components = {
    h3: ({ children }) => {
      const heading = Children.toArray(children)
        .filter((child): child is string => typeof child === "string")
        .join("");
      const severityClass =
        isIssuesPanel && heading === "Critical Errors"
          ? "text-[#ff6b6b]"
          : isIssuesPanel && heading === "Warnings"
            ? "text-[#fbbf24]"
            : "text-white";

      return (
        <h3 className={`mt-5 mb-2 first:mt-0 font-semibold ${severityClass}`}>
          {children}
        </h3>
      );
    },
    p: ({ children }) => (
      <p
        className={cn(
          "mb-4 last:mb-0",
          isKeyboardShortcuts && "text-xs text-slate-400",
        )}
      >
        {children}
      </p>
    ),
    strong: ({ children }) => (
      <strong className="text-white">{children}</strong>
    ),
    a: ({ children, href }) => {
      const className = cn(
        "text-accent hover:text-blue-400 transition font-mono text-sm",
        !isOverview && "underline",
      );

      return href?.endsWith("/SKILL.md") ? (
        <a href={href} download="SKILL.md" className={className}>
          {children}
        </a>
      ) : (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className={className}
        >
          {children}
        </a>
      );
    },
    code: ({ children }) =>
      isKeyboardShortcuts ? (
        <kbd className="bg-surface-hover border border-[#3d4a5c] text-[#7dd3fc] text-xs font-mono px-2 py-0.5 rounded">
          {children}
        </kbd>
      ) : (
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm font-mono">
          {children}
        </code>
      ),
    pre: ({ children }) => (
      <pre className="mb-4 last:mb-0 bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap [&_code]:bg-transparent [&_code]:p-0">
        {children}
      </pre>
    ),
    ul: ({ children }) => (
      <ul
        className={cn(
          "mb-4 last:mb-0 space-y-2 list-disc pl-5",
          isKeyboardShortcuts && "space-y-0 list-none pl-0",
          isOverview &&
            "border-t border-edge pt-4 flex flex-col gap-2 space-y-0 list-none pl-0 text-xs",
        )}
      >
        {children}
      </ul>
    ),
    li: ({ children }) => (
      <li
        className={cn(
          isKeyboardShortcuts &&
            "flex items-center gap-3 py-1.5 border-b border-[#1e2736] text-sm text-slate-300",
          isOverview &&
            "flex items-center justify-between text-slate-400 [&_strong]:text-slate-500 [&_strong]:uppercase [&_strong]:tracking-wider [&_strong]:text-tiny [&_strong]:font-semibold",
        )}
      >
        {children}
      </li>
    ),
  };

  return <ReactMarkdown components={components}>{markdown}</ReactMarkdown>;
};

export default MarkdownContent;
