const ISSUE_STYLES = {
  error: {
    container: "border-red-500/60 bg-red-950/55",
    label: "text-red-300",
    message: "text-red-100",
    divider: "divide-red-500/30",
  },
  warning: {
    container: "border-yellow-500/60 bg-yellow-950/45",
    label: "text-yellow-300",
    message: "text-yellow-100",
    divider: "divide-yellow-500/30",
  },
};

export default function PanelIssueNotice({ type, children }) {
  const styles = ISSUE_STYLES[type];
  if (!styles || children == null || children === "") return null;
  const messages = Array.isArray(children) ? children : [children];
  if (messages.length === 0) return null;

  return (
    <div
      className={`rounded-lg border px-3 py-2.5 ${styles.container}`}
      role={type === "error" ? "alert" : "status"}
    >
      <div
        className={`mb-1 text-xs font-bold uppercase tracking-wide ${styles.label}`}
      >
        {messages.length > 1 ? `${type}s` : type}
      </div>
      <div
        className={`divide-y whitespace-pre-wrap break-words font-mono text-xs leading-relaxed ${styles.divider} ${styles.message}`}
      >
        {messages.map((message, i) => (
          <div key={i} className="py-2 first:pt-0 last:pb-0">
            {String(message)}
          </div>
        ))}
      </div>
    </div>
  );
}
