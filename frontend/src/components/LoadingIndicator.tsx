interface LoadingIndicatorProps {
  title: string;
  description: string;
}

const LoadingIndicator = ({ title, description }: LoadingIndicatorProps) => (
  <div
    className="flex max-w-sm flex-col items-center text-center"
    role="status"
    aria-live="polite"
  >
    <div className="relative mb-6 flex h-16 w-16 items-center justify-center">
      <div className="absolute inset-0 rounded-full bg-accent/15 blur-md" />
      <div className="absolute inset-1 rounded-full border border-edge" />
      <div className="absolute inset-1 animate-spin rounded-full border-2 border-transparent border-t-accent border-r-accent/40 motion-reduce:animate-none" />
      <div className="h-2 w-2 rounded-full bg-accent shadow-lg shadow-accent/50" />
    </div>
    <p className="text-base font-semibold text-ink-bright">{title}</p>
    <p className="mt-2 text-sm leading-6 text-slate-400">{description}</p>
    <span className="sr-only">Loading</span>
  </div>
);

export default LoadingIndicator;
