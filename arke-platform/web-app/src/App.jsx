import { GeologicalTimeline } from "./GeologicalTimeline";
import { ArchaeologicalTimeline } from "./ArchaeologicalTimeline";
import "./GeologicalTimeline.css";

// Vite `--mode` is available at runtime via `import.meta.env.MODE`.
// This avoids relying on global compile-time flags that may not be applied by all transforms.
const isArkeoGIS = import.meta.env.MODE === "arkeogis";
const target = isArkeoGIS ? "ArkeoGIS" : "ArkeOpen";
const role = isArkeoGIS ? "research platform" : "open-data platform";

export function App() {
  return (
    <main className="shell">
      <section className="panel">
        <p className="eyebrow">{role}</p>
        <h1>{target}</h1>
        <p>
          Shared monorepo starter with product-specific build flags and separate
          `web-app`, `web-admin`, and `server` packages.
        </p>
      </section>
      
      <div className="timeline-wrapper">
        <GeologicalTimeline />
        <ArchaeologicalTimeline />
      </div>
    </main>
  );
}
