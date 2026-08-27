import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";

interface MarkdownContentProps {
  markdown: string;
}

const markdownComponents: Components = {
  h3: ({ children }) => (
    <h3 className="text-white font-semibold">{children}</h3>
  ),
  p: ({ children }) => <p>{children}</p>,
  strong: ({ children }) => <strong className="text-white">{children}</strong>,
  a: ({ children, href }) => {
    const className =
      "text-accent hover:text-blue-400 transition font-mono text-sm underline";

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
  code: ({ children }) => (
    <code className="text-[#7dd3fc] font-mono">{children}</code>
  ),
  pre: ({ children }) => (
    <pre className="bg-surface-hover rounded-md p-3 text-sm overflow-x-auto whitespace-pre-wrap">
      {children}
    </pre>
  ),
  ul: ({ children }) => <ul className="space-y-2 list-none">{children}</ul>,
  li: ({ children }) => (
    <li className="flex gap-2 before:content-['•'] before:text-accent">
      <span>{children}</span>
    </li>
  ),
};

const MarkdownContent = ({ markdown }: MarkdownContentProps) => (
  <ReactMarkdown components={markdownComponents}>{markdown}</ReactMarkdown>
);

export default MarkdownContent;
