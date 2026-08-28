const SchemaFieldHeader = () => (
  <div className="grid grid-cols-[1rem_2.5rem_minmax(0,1fr)_auto] items-center gap-x-3 border-b border-edge pb-1">
    <span />
    <span className="text-right text-xs font-bold uppercase text-slate-500">
      ID
    </span>
    <span className="text-xs font-bold uppercase text-slate-500">Name</span>
    <span className="text-xs font-bold uppercase text-slate-500">Required</span>
  </div>
);

export default SchemaFieldHeader;
