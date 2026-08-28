declare module "*.mdx" {
  import type { MDXProps } from "mdx/types";
  import type { ReactElement } from "react";

  const MdxContent: (props: MDXProps) => ReactElement;
  export default MdxContent;

  export const searchText: string;
}
