/** @type {import('next').NextConfig} */
const nextConfig = {
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // Note: Rewrites removed - frontend will call backend API directly
  // Backend has CORS configured to allow cross-origin requests
  // Use NEXT_PUBLIC_API_URL environment variable to configure backend URL
}

export default nextConfig
