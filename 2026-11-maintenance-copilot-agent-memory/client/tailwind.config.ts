import typography from "@tailwindcss/typography";
import type { Config } from "tailwindcss";

// DuBois: Lava is the only accent; navy + oat carry structure.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        lava: { DEFAULT: "#FF3621", 600: "#E02E1A", 50: "#FFF1EE", 100: "#FFE0D9" },
        navy: { DEFAULT: "#1B3139", 800: "#22414B", 600: "#4A6670", 400: "#7A8F96", 200: "#C4CED1" },
        oat: { DEFAULT: "#EEEDE9", 50: "#F9F8F6", 100: "#F5F4F1", 300: "#E3E1DB", 400: "#D3D0C8" },
        moss: { DEFAULT: "#00A972", 50: "#E6F6F0" },
        sky: { DEFAULT: "#2272B4", 50: "#E8F1F8" },
      },
      fontFamily: {
        sans: ['"DM Sans"', "ui-sans-serif", "sans-serif"],
        mono: ['"DM Mono"', "ui-monospace", "monospace"],
      },
      fontSize: { "2xs": ["0.6875rem", "1rem"] },
      boxShadow: { card: "0 1px 2px rgba(27,49,57,0.06), 0 1px 1px rgba(27,49,57,0.04)" },
      keyframes: {
        shimmer: { "100%": { transform: "translateX(100%)" } },
        pulseDot: { "0%,100%": { opacity: "1" }, "50%": { opacity: "0.35" } },
      },
      animation: { shimmer: "shimmer 1.4s infinite", "pulse-dot": "pulseDot 1.6s ease-in-out infinite" },
    },
  },
  plugins: [typography],
} satisfies Config;
