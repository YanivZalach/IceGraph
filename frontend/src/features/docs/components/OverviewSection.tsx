import { APP_VERSION } from "../../../appConstants";

const OverviewSection = () => (
  <div className="space-y-4">
    <p>
      IceGraph is an open source Apache Iceberg debugging and visualization
      platform. Trace production Iceberg tables through a graph based UI built
      for debugging complex metadata states, analyzing table evolution, and
      learning how Iceberg works under the hood.
    </p>
    <p>
      Everything is <strong className="text-white">read-only</strong>.
    </p>
    <div className="border-t border-edge pt-4 flex flex-col gap-2 text-xs">
      <div className="flex items-center justify-between text-slate-400">
        <span className="text-slate-500 uppercase tracking-wider text-tiny font-semibold">
          Version
        </span>
        <span className="font-mono text-slate-300">{APP_VERSION}</span>
      </div>
      <div className="flex items-center justify-between text-slate-400">
        <span className="text-slate-500 uppercase tracking-wider text-tiny font-semibold">
          Website
        </span>
        <a
          href="https://yanivzalach.github.io/IceGraph-Site/"
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent hover:text-blue-400 transition font-mono"
        >
          yanivzalach.github.io/IceGraph-Site
        </a>
      </div>
      <div className="flex items-center justify-between text-slate-400">
        <span className="text-slate-500 uppercase tracking-wider text-tiny font-semibold">
          Source
        </span>
        <a
          href="https://github.com/YanivZalach/IceGraph"
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent hover:text-blue-400 transition font-mono"
        >
          github.com/YanivZalach/IceGraph
        </a>
      </div>
    </div>
  </div>
);

export default OverviewSection;
