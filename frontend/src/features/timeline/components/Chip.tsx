import { cn } from "../../../shared/lib/cn";

export const CHIP_BASE_CLASS =
  "inline-block rounded-md px-2 py-px font-mono text-xs whitespace-nowrap";

export type ChipTone = "added" | "removed";

const TONE_CLASS: Record<ChipTone, string> = {
  added: "bg-green-500/15 text-green-400",
  removed: "bg-red-500/15 text-red-400",
};

interface ChipProps {
  text: string;
  tone: ChipTone;
}

const Chip = ({ text, tone }: ChipProps) => (
  <span className={cn(CHIP_BASE_CLASS, TONE_CLASS[tone])}>{text}</span>
);

export default Chip;
