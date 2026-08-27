import { formatByteSize } from "../../lib/format/backendSize";

interface ImpactSizeProps {
  netBytes: number;
}

const ImpactSize = ({ netBytes }: ImpactSizeProps) =>
  netBytes > 0 ? (
    <span className="text-green-400">+{formatByteSize(netBytes)}</span>
  ) : (
    <span className="text-red-400">−{formatByteSize(-netBytes)}</span>
  );

export default ImpactSize;
