interface KeyProps {
  k: string;
}

const Key = ({ k }: KeyProps) => (
  <kbd className="bg-surface-hover border border-[#3d4a5c] text-[#7dd3fc] text-xs font-mono px-2 py-0.5 rounded">
    {k}
  </kbd>
);

export default Key;
