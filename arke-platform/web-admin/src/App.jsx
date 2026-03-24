const target = __ARKEOGIS__ ? "ArkeoGIS admin" : "ArkeOpen admin";
const copy = __ARKEOGIS__
  ? "Editorial and research workflows for protected datasets."
  : "Publishing and curation workflows for open collections.";

export function App() {
  return (
    <main className="shell">
      <section className="panel">
        <p className="eyebrow">backoffice</p>
        <h1>{target}</h1>
        <p>{copy}</p>
      </section>
    </main>
  );
}

