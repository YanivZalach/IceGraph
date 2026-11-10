const MIN_READABLE_TEXT_PX = 4;

export interface FrameBounds {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
  globalScale: number;
}

export const UNBOUNDED_FRAME: FrameBounds = {
  minX: -Infinity,
  minY: -Infinity,
  maxX: Infinity,
  maxY: Infinity,
  globalScale: 1,
};

export const readFrameBounds = (
  context: CanvasRenderingContext2D,
  globalScale: number,
): FrameBounds => {
  const inverse = context.getTransform().inverse();
  const topLeft = inverse.transformPoint({ x: 0, y: 0 });
  const bottomRight = inverse.transformPoint({
    x: context.canvas.width,
    y: context.canvas.height,
  });
  return {
    minX: topLeft.x,
    minY: topLeft.y,
    maxX: bottomRight.x,
    maxY: bottomRight.y,
    globalScale,
  };
};

export const isBoxInFrame = (
  bounds: FrameBounds,
  minX: number,
  minY: number,
  maxX: number,
  maxY: number,
): boolean =>
  maxX >= bounds.minX &&
  minX <= bounds.maxX &&
  maxY >= bounds.minY &&
  minY <= bounds.maxY;

export const isTextReadable = (
  bounds: FrameBounds,
  fontSize: number,
): boolean => fontSize * bounds.globalScale >= MIN_READABLE_TEXT_PX;
