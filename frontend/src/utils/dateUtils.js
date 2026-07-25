export function parseUtcDate(tsStr) {
  if (!tsStr) return null;
  // If the string does not end with Z or a timezone offset like +00:00, append Z to force UTC parsing
  let normalized = tsStr;
  if (!tsStr.endsWith('Z') && !/[+-]\d{2}:\d{2}$/.test(tsStr)) {
    normalized = tsStr + 'Z';
  }
  const d = new Date(normalized);
  return isNaN(d.getTime()) ? null : d;
}

export function formatUtcOffset(dateObj) {
  const offset = -dateObj.getTimezoneOffset()
  const sign = offset >= 0 ? '+' : '-'
  const pad = (n, len = 2) => String(n).padStart(len, '0')
  return `${sign}${pad(Math.floor(Math.abs(offset) / 60))}:${pad(Math.abs(offset) % 60)}`
}

export function formatLocaleDateTime(dateObj) {
  if (!dateObj) return ''

  const pad = (n, len = 2) => String(n).padStart(len, '0')

  return `${dateObj.getFullYear()}-${pad(dateObj.getMonth() + 1)}-${pad(dateObj.getDate())} ${pad(dateObj.getHours())}:${pad(dateObj.getMinutes())}:${pad(dateObj.getSeconds())}.${pad(dateObj.getMilliseconds(), 3)}${formatUtcOffset(dateObj)}`
}