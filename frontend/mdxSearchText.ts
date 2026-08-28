import { toString } from "mdast-util-to-string";
import { visit } from "unist-util-visit";
import type { MdxjsEsm } from "mdast-util-mdx";
import type { Nodes, Root } from "mdast";

const MDX_EXPRESSIONS = new Set(["mdxFlowExpression", "mdxTextExpression"]);
const PROSE_BLOCKS = new Set([
  "paragraph",
  "heading",
  "code",
  "listItem",
  "mdxJsxFlowElement",
]);

const stringLiteralOf = (node: Nodes): string | undefined => {
  if (node.type !== "mdxFlowExpression" && node.type !== "mdxTextExpression") {
    return undefined;
  }

  const statement = node.data?.estree?.body[0];
  if (statement?.type !== "ExpressionStatement") return undefined;

  const { expression } = statement;
  return expression.type === "Literal" && typeof expression.value === "string"
    ? expression.value
    : undefined;
};

const collectProse = (node: Nodes, blocks: string[]): void => {
  if (PROSE_BLOCKS.has(node.type)) {
    blocks.push(toString(node));
    return;
  }
  if ("children" in node) {
    node.children.forEach((child) => {
      collectProse(child, blocks);
    });
  }
};

const extractSearchText = (tree: Root): string => {
  /** Extract row text from tree */
  const prose = structuredClone(tree);

  visit(prose, (node, index, parent) => {
    if (!parent || index === undefined) return undefined;

    if (MDX_EXPRESSIONS.has(node.type)) {
      const literal = stringLiteralOf(node);
      if (literal === undefined) {
        parent.children.splice(index, 1);
        return index;
      }
      parent.children[index] = { type: "text", value: literal };
    }

    return undefined;
  });

  const blocks: string[] = [];
  collectProse(prose, blocks);

  return blocks.join(" ").replace(/\s+/g, " ").trim();
};

const searchTextExport = (text: string): MdxjsEsm => ({
  /** Makes the text importable as `searchText` in MDX components. */
  type: "mdxjsEsm",
  value: "",
  data: {
    estree: {
      type: "Program",
      sourceType: "module",
      body: [
        {
          type: "ExportNamedDeclaration",
          specifiers: [],
          source: null,
          attributes: [],
          declaration: {
            type: "VariableDeclaration",
            kind: "const",
            declarations: [
              {
                type: "VariableDeclarator",
                id: { type: "Identifier", name: "searchText" },
                init: { type: "Literal", value: text },
              },
            ],
          },
        },
      ],
    },
  },
});

export const remarkSearchText = () => (tree: Root) => {
  tree.children.unshift(searchTextExport(extractSearchText(tree)));
};
