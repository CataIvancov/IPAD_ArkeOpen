import express from "express";

const app = express();
const port = Number(process.env.PORT || 40100);

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "arkeoserver" });
});

app.listen(port, () => {
  console.log(`arkeoserver listening on ${port}`);
});

