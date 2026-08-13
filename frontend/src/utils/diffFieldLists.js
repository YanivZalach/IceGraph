export function diffFieldLists(prevFields, currFields, idKeyFn) {
  const prevById = Object.fromEntries(
    (prevFields ?? []).map((f) => [idKeyFn(f), f]),
  );
  const currById = Object.fromEntries(
    (currFields ?? []).map((f) => [idKeyFn(f), f]),
  );
  const allIds = Array.from(
    new Set([...Object.keys(prevById), ...Object.keys(currById)]),
  );

  return allIds.map((id) => {
    const prev = prevById[id];
    const curr = currById[id];
    if (!prev) return { status: "added", field: curr };
    if (!curr) return { status: "removed", field: prev };
    if (JSON.stringify(prev) !== JSON.stringify(curr))
      return { status: "changed", prev, curr };
    return { status: "unchanged", field: curr };
  });
}
