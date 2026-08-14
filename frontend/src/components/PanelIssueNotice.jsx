const ISSUE_STYLES = {
  error: {
    container: "border-red-500/60 bg-red-950/55",
    label: "text-red-300",
    message: "text-red-100",
  },
  warning: {
    container: "border-yellow-500/60 bg-yellow-950/45",
    label: "text-yellow-300",
    message: "text-yellow-100",
  },
};

export default function PanelIssueNotice({ type, children }) {
  const styles = ISSUE_STYLES[type];
  if (!styles || children == null || children === "") return null;

  return (
    <div
      className={`rounded-lg border px-3 py-2.5 ${styles.container}`}
      role={type === "error" ? "alert" : "status"}
    >
      <div
        className={`mb-1 text-xs font-bold uppercase tracking-wide ${styles.label}`}
      >
        {type}
      </div>
      <div
        className={`whitespace-pre-wrap break-words font-mono text-xs leading-relaxed ${styles.message}`}
      >
        {String(children)}
      </div>
    </div>
  );
}
