import Key from "./Key";
import ShortcutRow from "./ShortcutRow";

const KeyboardShortcutsSection = () => (
  <div className="space-y-6">
    <div className="space-y-1">
      <h3 className="text-white font-semibold mb-2">Global</h3>
      <ShortcutRow keys={["1"]} desc="Go to Timeline view" />
      <ShortcutRow keys={["2"]} desc="Go to Metadata view" />
      <ShortcutRow keys={["3"]} desc="Go to FileTree view" />
      <ShortcutRow keys={["4"]} desc="Go to Graph view" />
    </div>

    <p className="text-slate-400 text-xs">
      Throughout the app, <Key k="j" /> / <Key k="↓" /> and <Key k="k" /> /{" "}
      <Key k="↑" /> scroll the active panel or list.
    </p>

    <div className="space-y-1">
      <h3 className="text-white font-semibold mb-2">Docs Page</h3>
      <ShortcutRow keys={["k"]} desc="Open the documentation search overlay" />
      <ShortcutRow keys={["Ctrl", "n"]} desc="Select the next search result" />
      <ShortcutRow
        keys={["Ctrl", "p"]}
        desc="Select the previous search result"
      />
      <ShortcutRow keys={["Enter"]} desc="Open the selected search result" />
      <ShortcutRow keys={["Esc"]} desc="Close the search overlay" />
    </div>

    <div className="space-y-1">
      <h3 className="text-white font-semibold mb-2">Timeline View</h3>
      <ShortcutRow
        keys={["Shift", "Scroll"]}
        desc="Pan the timeline horizontally"
      />
      <ShortcutRow
        keys={["r"]}
        desc="Center and zoom to fit the entire timeline"
      />
      <ShortcutRow
        keys={["h", "←"]}
        desc="Select the previous snapshot - if none is selected, jumps to the first (oldest)"
      />
      <ShortcutRow
        keys={["l", "→"]}
        desc="Select the next snapshot - if none is selected, jumps to the last (newest)"
      />
      <ShortcutRow
        keys={["j", "↓"]}
        desc="Scroll the snapshot details panel down"
      />
      <ShortcutRow
        keys={["k", "↑"]}
        desc="Scroll the snapshot details panel up"
      />
      <ShortcutRow
        keys={["f"]}
        desc="Toggle fullscreen for the snapshot details panel (when open)"
      />
      <ShortcutRow keys={["Esc"]} desc="Close the snapshot details panel" />
    </div>

    <div className="space-y-1">
      <h3 className="text-white font-semibold mb-2">Metadata View</h3>
      <ShortcutRow keys={["j", "↓"]} desc="Scroll the page down" />
      <ShortcutRow keys={["k", "↑"]} desc="Scroll the page up" />
    </div>

    <div className="space-y-1">
      <h3 className="text-white font-semibold mb-2">Graph View</h3>
      <ShortcutRow
        keys={["c"]}
        desc="Center and zoom to fit the entire graph"
      />
      <ShortcutRow keys={["r"]} desc="Reset view to initial state" />
      <ShortcutRow
        keys={["i"]}
        desc="Toggle inspect mode (disables keyboard navigation so you can interact freely with the graph)"
      />
      <ShortcutRow
        keys={["Enter", "Space"]}
        desc="Jump to the main metadata node"
      />
      <ShortcutRow
        keys={["h", "←"]}
        desc="Navigate to the selected node's parent(s)"
      />
      <ShortcutRow
        keys={["l", "→"]}
        desc="Navigate to the selected node's child(ren)"
      />
      <ShortcutRow
        keys={["j", "↓"]}
        desc="Scroll the node details panel down (when open)"
      />
      <ShortcutRow
        keys={["k", "↑"]}
        desc="Scroll the node details panel up (when open)"
      />
      <ShortcutRow
        keys={["f"]}
        desc="Toggle fullscreen for the details panel (when open)"
      />
      <ShortcutRow keys={["Esc"]} desc="Close the node details panel" />
    </div>
  </div>
);

export default KeyboardShortcutsSection;
