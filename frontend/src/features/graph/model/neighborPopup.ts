import type { GraphNode } from "./graphModel";
import type { NeighborDirection } from "./lineage";

export const NEIGHBOR_POPUP_KEYS = "abdegmnopqstuvwxyz";

export interface NeighborChoice {
  node: GraphNode;
  combo: string;
}

export interface NeighborPopupState {
  direction: NeighborDirection;
  choices: NeighborChoice[];
  comboLength: number;
}

const comboForIndex = (index: number, comboLength: number): string => {
  let combo = "";
  let remaining = index;
  for (let position = 0; position < comboLength; position++) {
    combo = `${NEIGHBOR_POPUP_KEYS.charAt(remaining % NEIGHBOR_POPUP_KEYS.length)}${combo}`;
    remaining = Math.floor(remaining / NEIGHBOR_POPUP_KEYS.length);
  }
  return combo;
};

export const buildNeighborPopup = (
  nodes: GraphNode[],
  direction: NeighborDirection,
): NeighborPopupState => {
  const comboLength = Math.floor(nodes.length / NEIGHBOR_POPUP_KEYS.length) + 1;
  return {
    direction,
    comboLength,
    choices: nodes.map((node, index) => ({
      node,
      combo: comboForIndex(index, comboLength),
    })),
  };
};

export const findChoiceByCombo = (
  popup: NeighborPopupState,
  combo: string,
): NeighborChoice | null =>
  popup.choices.find((choice) => choice.combo === combo) ?? null;
