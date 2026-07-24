const JSON_TOKEN_REGEX = /("(?:\\u[a-fA-F0-9]{4}|\\.|[^"\\])*"(\s*:)?)|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g

export function highlightJson(text) {
  const nodes = []
  let lastIndex = 0
  let match
  let key = 0

  JSON_TOKEN_REGEX.lastIndex = 0
  while ((match = JSON_TOKEN_REGEX.exec(text)) !== null) {
    if (match.index > lastIndex) {
      nodes.push(<span key={key++}>{text.slice(lastIndex, match.index)}</span>)
    }
    const [full, strMatch, colonPart, boolMatch, nullMatch, numMatch] = match
    let className = 'text-slate-300'
    if (strMatch) className = colonPart ? 'text-accent' : 'text-emerald-400'
    else if (boolMatch) className = 'text-violet-400'
    else if (nullMatch) className = 'text-slate-500'
    else if (numMatch) className = 'text-amber-400'
    nodes.push(<span key={key++} className={className}>{full}</span>)
    lastIndex = JSON_TOKEN_REGEX.lastIndex
  }
  if (lastIndex < text.length) {
    nodes.push(<span key={key++}>{text.slice(lastIndex)}</span>)
  }
  return nodes
}
