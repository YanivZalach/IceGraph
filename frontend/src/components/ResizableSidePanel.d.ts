import type {
  ForwardRefExoticComponent,
  ReactNode,
  RefAttributes,
} from "react";

// Hand-written for the untyped ResizableSidePanel.jsx — nothing checks they
// agree, so update BOTH together. Dies when the component goes TS:
// https://github.com/YanivZalach/IceGraph/issues/100
interface ResizableSidePanelProps {
  accentColor: string;
  header: ReactNode;
  children: ReactNode;
  onClose: () => void;
  onLayoutChange?: (layout: {
    isFullscreen: boolean;
    panelWidthRem: number;
  }) => void;
  maxContainerWidth?: number;
}

declare const ResizableSidePanel: ForwardRefExoticComponent<
  ResizableSidePanelProps & RefAttributes<HTMLDivElement>
>;

export default ResizableSidePanel;
export declare const PANEL_WIDTH_RELAXED: number;
