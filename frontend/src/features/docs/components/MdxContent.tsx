import type { MDXComponents } from "mdx/types";
import type { ComponentType, ReactNode } from "react";
import { APP_VERSION, BASE_PATH } from "../../../appConstants";
import { cn } from "../../../shared/lib/cn";

interface MdxContentProps {
  Content: ComponentType<{ components?: MDXComponents }>;
  sectionId: string;
}

interface ChildrenProps {
  children?: ReactNode;
}

interface AnchorProps extends ChildrenProps {
  href?: string;
}

const pipInstallCommand =
  APP_VERSION === "dev"
    ? "pip install icegraph-client"
    : `pip install icegraph-client==${APP_VERSION.replace(/^v/, "")}`;

const MdxContent = ({ Content, sectionId }: MdxContentProps) => {
  const isKeyboardShortcuts = sectionId === "keyboard-shortcuts";
  const isOverview = sectionId === "overview";

  const components: MDXComponents = {
    h3: ({ children }) => (
      <h3 className="mt-5 mb-2 first:mt-0 text-white font-semibold">
        {children}
      </h3>
    ),
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
    a: ({ children, href }: AnchorProps) => {
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
    AppVersion: () => APP_VERSION,
    PipInstallCommand: () => (
      <pre className="mb-4 last:mb-0 bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">
        <code>{pipInstallCommand}</code>
      </pre>
    ),
    SkillDownloadLink: ({ children }: ChildrenProps) => (
      <a
        href={`${BASE_PATH}/SKILL.md`}
        download="SKILL.md"
        className="text-accent hover:text-blue-400 transition font-mono text-sm underline"
      >
        {children}
      </a>
    ),
    CriticalHeading: ({ children }: ChildrenProps) => (
      <h3 className="mt-5 mb-2 first:mt-0 font-semibold text-[#ff6b6b]">
        {children}
      </h3>
    ),
    WarningHeading: ({ children }: ChildrenProps) => (
      <h3 className="mt-5 mb-2 first:mt-0 font-semibold text-[#fbbf24]">
        {children}
      </h3>
    ),
  };

  return <Content components={components} />;
};

export default MdxContent;
