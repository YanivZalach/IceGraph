import { cn } from "../../../shared/lib/cn";

export const CHIP_BASE_CLASS =
  "inline-block rounded px-2 py-0.5 font-mono text-base whitespace-nowrap";

export type ChipTone = "added" | "removed";

const TONE_CLASS: Record<ChipTone, string> = {
  added: "bg-green-500/15 text-green-400",
  removed: "bg-red-500/15 text-red-400",
};

interface ChipProps {
  text: string;
  tone: ChipTone;
  isCompact?: boolean;
}

const Chip = ({ text, tone, isCompact = false }: ChipProps) => (
  <span
    className={cn(CHIP_BASE_CLASS, isCompact && "text-sm", TONE_CLASS[tone])}
  >
    {text}
  </span>
);

export default Chip;
