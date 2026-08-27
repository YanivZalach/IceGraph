const TipsSection = () => (
  <div className="space-y-5">
    <div className="space-y-2">
      <h3 className="text-white font-semibold">
        Start with a narrow snapshot range
      </h3>
      <p>
        Loading all snapshots at once produces an overwhelming graph. Start with
        the 2–7 most recent snapshots and expand only if you need more history.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">
        Duplicate tab for side-by-side comparison
      </h3>
      <p>
        Use the <strong className="text-white">Duplicate tab</strong> button in
        the navbar to open the current view in a new browser tab using cached
        data - no extra backend request. Load a different snapshot range in the
        original tab to compare two states of the same table.
      </p>
    </div>
  </div>
);

export default TipsSection;
