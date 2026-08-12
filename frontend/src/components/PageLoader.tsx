import logo from "../assets/icegraph.png";

const PageLoader = () => (
  <div className="fixed inset-0 flex flex-col items-center justify-center gap-5 bg-canvas">
    <img src={logo} alt="IceGraph" className="h-28 w-28 object-contain" />
    <div className="flex items-center gap-1 text-lg font-medium tracking-wide text-slate-300">
      <span>Loading</span>
      <span className="animate-bounce [animation-delay:-0.3s]">.</span>
      <span className="animate-bounce [animation-delay:-0.15s]">.</span>
      <span className="animate-bounce">.</span>
    </div>
  </div>
);

export default PageLoader;
