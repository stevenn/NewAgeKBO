import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  webpack: (config, { isServer }) => {
    if (isServer) {
      // Externalize duckdb to prevent webpack from bundling native modules
      config.externals = config.externals || []
      config.externals.push('duckdb')
    }
    return config
  },
}

export default nextConfig
