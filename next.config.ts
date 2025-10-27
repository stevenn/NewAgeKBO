import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  // Externalize @duckdb/node-api to prevent webpack from bundling native modules
  serverExternalPackages: ['@duckdb/node-api'],

  webpack: (config) => {
    // Suppress webpack cache warnings for runtime-only caches
    // The codes cache is populated at runtime and doesn't need build-time serialization
    config.infrastructureLogging = {
      ...config.infrastructureLogging,
      level: 'error',
    }

    return config
  },
}

export default nextConfig
