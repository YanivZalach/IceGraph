import Markdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import { cn } from "../../../shared/lib/cn";

interface AssistantAnswerProps {
  content: string;
}

const REMARK_PLUGINS = [remarkGfm];

const MARKDOWN_COMPONENTS: Components = {
  a: ({ href, children }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-accent-text underline transition hover:text-blue-400"
    >
      {children}
    </a>
  ),
  // Model output must not make the browser fetch a URL without a click.
  img: ({ alt }) => <span>{alt}</span>,
  table: ({ children }) => (
    <div className="my-3 overflow-x-auto rounded-lg border border-edge">
      <table className="w-full border-collapse text-xs">{children}</table>
    </div>
  ),
};

const ANSWER_CLASS = cn(
  "min-w-0 text-sm leading-6 break-words text-slate-200",
  "[&>*:first-child]:mt-0 [&>*:last-child]:mb-0 [&_p]:my-2.5 [&_strong]:font-semibold [&_strong]:text-ink-bright",
  "[&_h1]:mt-3 [&_h1]:mb-1 [&_h1]:text-base [&_h1]:font-bold [&_h1]:text-ink-bright",
  "[&_h2]:mt-3 [&_h2]:mb-1 [&_h2]:text-sm [&_h2]:font-semibold [&_h2]:text-ink-bright",
  "[&_h3]:mt-3 [&_h3]:mb-1 [&_h3]:font-semibold [&_h3]:text-ink-bright",
  "[&_h4]:mt-2 [&_h4]:mb-1 [&_h4]:font-semibold [&_h4]:text-ink-bright [&_h5]:mt-3 [&_h5]:mb-1 [&_h5]:font-semibold [&_h6]:mt-3 [&_h6]:mb-1 [&_h6]:font-semibold",
  "[&_ul]:my-2 [&_ul]:list-disc [&_ul]:pl-5 [&_ol]:my-2 [&_ol]:list-decimal [&_ol]:pl-5 [&_li]:my-0.5 [&_li>p]:my-0.5 [&_li::marker]:text-slate-500",
  "[&_code]:rounded [&_code]:bg-surface-hover [&_code]:px-1 [&_code]:py-px [&_code]:font-mono [&_code]:text-[12px] [&_code]:text-sky-200",
  "[&_pre]:my-3 [&_pre]:overflow-x-auto [&_pre]:rounded-lg [&_pre]:border [&_pre]:border-edge/70 [&_pre]:bg-canvas/70 [&_pre]:p-3 [&_pre_code]:bg-transparent [&_pre_code]:p-0",
  "[&_th]:border-b [&_th]:border-edge [&_th]:bg-surface-hover/60 [&_th]:px-2 [&_th]:py-2 [&_th]:text-left [&_th]:text-ink-bright [&_td]:border-b [&_td]:border-edge/60 [&_td]:px-2 [&_td]:py-2 [&_tr:last-child_td]:border-b-0",
  "[&_blockquote]:my-2 [&_blockquote]:border-l-2 [&_blockquote]:border-edge [&_blockquote]:pl-3 [&_blockquote]:text-slate-400 [&_hr]:my-3 [&_hr]:border-edge",
);

const AssistantAnswer = ({ content }: AssistantAnswerProps) => (
  <div className={ANSWER_CLASS}>
    <Markdown remarkPlugins={REMARK_PLUGINS} components={MARKDOWN_COMPONENTS}>
      {content}
    </Markdown>
  </div>
);

export default AssistantAnswer;
