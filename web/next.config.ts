import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // The dashboard reads the funding-atlas data API entirely from the browser,
  // so no server-side secret is ever required and the app can be served as a
  // static bundle behind the same Caddy instance as the API.
  env: {},
};

export default nextConfig;
