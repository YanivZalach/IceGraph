import { useEffect, useState } from "react";
import { getNavHeightPx } from "../../../layoutConstants";
import {
  getGraphNodeMetrics,
  type GraphNodeMetrics,
} from "../canvas/nodeMetrics";

export interface GraphViewport {
  width: number;
  height: number;
  nodeMetrics: GraphNodeMetrics;
}

const measureViewport = (): GraphViewport => ({
  width: window.innerWidth,
  height: window.innerHeight - getNavHeightPx(),
  nodeMetrics: getGraphNodeMetrics(),
});

export const useGraphViewport = (): GraphViewport => {
  const [viewport, setViewport] = useState(measureViewport);

  useEffect(() => {
    const handleResize = (): void => {
      setViewport(measureViewport());
    };
    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  return viewport;
};
