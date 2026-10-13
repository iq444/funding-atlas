import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Static export: every page is a client component reading the data API from
  // the browser, so there is nothing for a Node process to do at request time.
  // Caddy serves the files directly, which keeps the host that is recording
  // market data 24/7 from also running an application server.
  output: "export",
  // Emits /asset/_/index.html rather than /asset/_.html, so one Caddy rewrite
  // covers the route.
  trailingSlash: true,
  // The dashboard reads the funding-atlas data API entirely from the browser,
  // so no server-side secret is ever required and the app can be served as a
  // static bundle behind the same Caddy instance as the API.
  env: {},
};

export default nextConfig;
