import "./globals.css";

export const metadata = {
  title: "Observability & Log Analytics Platform",
  description:
    "Microservice logs through Vector + Kafka into ClickHouse, with Airflow-scheduled z-score anomaly detection.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
