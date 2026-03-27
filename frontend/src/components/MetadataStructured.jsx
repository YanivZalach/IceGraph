export default function MetadataStructured({ metadata, onSelect, selectedId }) {
  const renderBoxes = (items, idKey, labelPrefix, activeId, type) => {
    if (!items || !Array.isArray(items)) return null
    return (
      <div className="mb-6">
        <div className="text-[0.65rem] font-black text-slate-400 uppercase tracking-[0.1em] mb-3 border-b border-slate-100 pb-1.5 flex justify-between items-center">
          <span>{labelPrefix} History</span>
          <span className="text-[0.6rem] font-medium normal-case bg-slate-100 px-2 py-0.5 rounded-full text-slate-500">
            {items.length} {items.length === 1 ? 'item' : 'items'}
          </span>
        </div>
        <div className="flex flex-wrap gap-2.5">
          {items.map((item) => {
            const id = item[idKey]
            const isActive = id === activeId
            const isSelected = selectedId === `${labelPrefix} ID: ${id}`

            return (
              <div
                key={id}
                className={`relative p-3 min-w-[70px] text-center cursor-pointer rounded-xl border-2 transition-all duration-200 group
                  ${isActive
                    ? 'border-[#2E86C1] bg-blue-50/50 shadow-sm'
                    : isSelected
                      ? 'border-amber-400 bg-amber-50 shadow-sm'
                      : 'border-slate-200 bg-white hover:border-slate-300 hover:shadow-sm'
                  }`}
                onClick={() => onSelect(type, id)}
                title={`Fields: ${item.fields ? item.fields.length : (item.partition_fields ? item.partition_fields.length : 0)}`}
              >
                {isActive && (
                  <span className="absolute -top-2.5 left-1/2 -translate-x-1/2 bg-[#2E86C1] text-white text-[0.55rem] font-black px-2 py-0.5 rounded-full shadow-sm tracking-wider z-10">
                    ACTIVE
                  </span>
                )}
                <span className={`text-[0.6rem] uppercase font-bold block mb-1 tracking-tighter
                  ${isActive ? 'text-[#2E86C1]' : isSelected ? 'text-amber-600' : 'text-slate-400'}
                `}>
                  {labelPrefix}
                </span>
                <span className={`block text-xl font-black leading-none
                  ${isActive ? 'text-slate-900' : 'text-slate-700'}
                `}>
                  {id}
                </span>

                {isSelected && (
                  <div className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-1.5 h-1.5 bg-amber-400 rounded-full" />
                )}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  return (
    <div className="py-2">
      {renderBoxes(metadata.schemas, 'schema-id', 'Schema', metadata['current-schema-id'], 'schema')}
      {renderBoxes(metadata['partition-specs'], 'spec-id', 'Spec', metadata['default-spec-id'], 'spec')}
      {renderBoxes(metadata['sort-orders'], 'order-id', 'Order', metadata['default-sort-order-id'], 'order')}
    </div>
  )
}
