import { z } from "zod";

const envSchema = z.object({
  BASE_URL: z.string().min(1),
  VITE_USE_MSW: z.enum(["true", "false"]).optional(),
  VITE_APP_VERSION: z.string().min(1).optional(),
});

const parsedEnv = envSchema.parse(import.meta.env);

interface Env {
  basePath: string;
  apiBaseUrl: string;
  isMock: boolean;
  appVersion: string;
}

export const env: Env = {
  basePath: parsedEnv.BASE_URL.replace(/\/$/, ""),
  apiBaseUrl: "/api/v1",
  isMock: parsedEnv.VITE_USE_MSW === "true",
  appVersion: parsedEnv.VITE_APP_VERSION ?? "dev",
};
