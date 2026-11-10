interface GraphCacheIssueNoticeProps {
  issue: string | null | undefined;
}

const GraphCacheIssueNotice = ({ issue }: GraphCacheIssueNoticeProps) => {
  if (issue === null || issue === undefined) return null;

  return (
    <p
      role="status"
      className="mb-6 rounded-lg border border-amber-800 bg-amber-950/40 p-3 text-sm break-words text-amber-300"
    >
      Could not verify the saved graph, so it is being rebuilt: {issue}
    </p>
  );
};
export default GraphCacheIssueNotice;
