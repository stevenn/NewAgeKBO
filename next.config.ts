import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Externalize duckdb to prevent webpack from bundling native modules
      config.externals = config.externals || []
      config.externals.push('duckdb')
    }

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
