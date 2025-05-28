import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Common configuration for all environments
  reactStrictMode: true,
  
  // For production build (to be served by FastAPI)
  ...(process.env.NODE_ENV === 'production' && {
    output: 'export',
    images: {
      unoptimized: true,
    },
    trailingSlash: true,
    
    // Static export doesn't support Server Actions
    serverActions: false,
  }),
  
  // Development configuration
  ...(process.env.NODE_ENV !== 'production' && {
    // Development rewrites to proxy API requests to FastAPI
    async rewrites() {
      return [
        {
          source: '/api/v1/:path*',
          destination: 'http://localhost:8000/api/v1/:path*',
        },
      ];
    },
  }),
  
};

export default nextConfig;