import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

export default defineConfig(({ mode }) => {
  const appTarget = mode === "arkeogis" ? "arkeogis" : "arkeopen";

  return {
    plugins: [react()],
    define: {
      __ARKEOPEN__: JSON.stringify(appTarget === "arkeopen"),
      __ARKEOGIS__: JSON.stringify(appTarget === "arkeogis")
    }
  };
});

