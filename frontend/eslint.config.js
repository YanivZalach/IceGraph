import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import eslintComments from "@eslint-community/eslint-plugin-eslint-comments";
import prettier from "eslint-config-prettier";

const philosophyRules = {
  "func-style": ["error", "expression"],
  "prefer-arrow-callback": "error",
  "no-restricted-imports": [
    "error",
    {
      paths: [
        {
          name: "react",
          importNames: ["memo", "useMemo", "useCallback"],
          message:
            "React Compiler handles memoization — see PHILOSOPHY.md 'Functions & components'.",
        },
      ],
    },
  ],
  "no-restricted-syntax": [
    "error",
    {
      selector: "TSEnumDeclaration",
      message:
        "Use union types or `as const` objects instead of enums — see PHILOSOPHY.md 'TypeScript'.",
    },
    {
      selector: "TSTypeAssertion",
      message:
        "Type assertions are banned except `as const` — see PHILOSOPHY.md 'TypeScript'.",
    },
    {
      selector: 'TSAsExpression:not([typeAnnotation.typeName.name="const"])',
      message:
        "Type assertions are banned except `as const` — see PHILOSOPHY.md 'TypeScript'.",
    },
  ],
};

export default tseslint.config(
  { ignores: ["dist", "public", "node_modules", "src/routeTree.gen.ts"] },

  // eslint-disable comments must carry a justification (PHILOSOPHY.md 'Tooling & enforcement')
  {
    files: ["**/*.{js,jsx,ts,tsx}"],
    plugins: { "@eslint-community/eslint-comments": eslintComments },
    rules: {
      "@eslint-community/eslint-comments/require-description": "error",
    },
  },

  // New code: full philosophy strictness
  {
    files: ["**/*.{ts,tsx}"],
    extends: [
      js.configs.recommended,
      tseslint.configs.strictTypeChecked,
      reactRefresh.configs.vite,
    ],
    plugins: { "react-hooks": reactHooks },
    languageOptions: {
      globals: globals.browser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      ...reactHooks.configs.flat["recommended-latest"].rules,
      ...philosophyRules,
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/consistent-type-definitions": ["error", "interface"],
      "@typescript-eslint/ban-ts-comment": [
        "error",
        {
          "ts-ignore": true,
          "ts-expect-error": { descriptionFormat: "^.*(https?://|#\\d+).*$" },
        },
      ],
    },
  },

  // TanStack Router file routes: exporting `Route` and throwing redirect()
  // are the router's own documented pattern —
  // https://tanstack.com/router/latest/docs/framework/react/routing/file-based-routing
  {
    files: ["src/routes/**/*.tsx"],
    rules: {
      "react-refresh/only-export-components": [
        "error",
        { allowExportNames: ["Route"] },
      ],
      "@typescript-eslint/only-throw-error": "off",
    },
  },

  // Component files: 200-line cap (lint-enforced per PHILOSOPHY.md)
  {
    files: ["**/*.tsx"],
    rules: {
      "max-lines": [
        "error",
        { max: 200, skipBlankLines: true, skipComments: true },
      ],
    },
  },

  // Legacy pre-philosophy code: correctness rules only, tightens as files migrate to .tsx
  {
    files: ["**/*.{js,jsx}"],
    extends: [js.configs.recommended],
    plugins: { "react-hooks": reactHooks },
    languageOptions: {
      globals: { ...globals.browser, ...globals.node },
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
      "no-useless-assignment": "off",
    },
  },

  prettier,
);
