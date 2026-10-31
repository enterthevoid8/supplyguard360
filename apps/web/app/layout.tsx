import './globals.css';
import type { Metadata } from 'next';
export const metadata: Metadata = {
  title: 'SupplyGuard 360 | Supply Chain Risk Intelligence',
  description: 'Real-time inventory and fulfillment intelligence powered by Databricks',
};
export default function RootLayout({ children }: Readonly<{children: React.ReactNode}>) {
  return <html lang="en"><body>{children}</body></html>;
}
