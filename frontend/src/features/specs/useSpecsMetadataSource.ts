import { useRouterState } from "@tanstack/react-router";

interface SpecsMetadataSource {
  isTablePage: boolean;
  shouldUseCurrentMetadata: boolean;
}

export const useSpecsMetadataSource = (): SpecsMetadataSource => {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  return {
    isTablePage: pathname.startsWith("/table/"),
    shouldUseCurrentMetadata:
      pathname === "/snapshots-selection" || pathname.startsWith("/partition/"),
  };
};
