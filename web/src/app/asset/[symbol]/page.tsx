import AssetPage from "./asset-page";

/**
 * One placeholder path for the whole route.
 *
 * A static export has to know every path at build time, but the listed perps
 * change whenever the venue lists or delists one — so generating a page per
 * asset would be stale by the next listing and would need a rebuild to fix.
 * Instead a single page is emitted and Caddy rewrites /asset/<symbol> onto it;
 * the component reads the symbol from the URL in the browser.
 */
export function generateStaticParams() {
  return [{ symbol: "_" }];
}

export default function Page() {
  return <AssetPage />;
}
