import { formatByteSize } from "../../lib/format/backendSize";
import Chip from "../Chip";

interface ImpactSizeProps {
  netBytes: number;
}

const ImpactSize = ({ netBytes }: ImpactSizeProps) =>
  netBytes > 0 ? (
    <Chip text={`+${formatByteSize(netBytes)}`} tone="added" />
  ) : (
    <Chip text={`−${formatByteSize(-netBytes)}`} tone="removed" />
  );

export default ImpactSize;
