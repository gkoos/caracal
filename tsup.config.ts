import { defineConfig } from "tsup"

export default defineConfig({
  entry: {
    index: "src/index.ts",
    redis: "src/redis.ts",
    fetch: "src/fetch.ts",
    postgres: "src/postgres.ts",
    "testing/index": "test/harness/index.ts",
  },
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: true,
  treeshake: true,
})
