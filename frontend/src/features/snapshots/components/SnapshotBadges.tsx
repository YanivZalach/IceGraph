interface SnapshotBadgesProps {
  badges: readonly string[];
}

const SnapshotBadges = ({ badges }: SnapshotBadgesProps) =>
  badges.map((badge) => (
    <span
      key={badge}
      className="shrink-0 rounded-md bg-accent px-2 py-px text-xs font-bold text-white"
    >
      {badge}
    </span>
  ));

export default SnapshotBadges;
