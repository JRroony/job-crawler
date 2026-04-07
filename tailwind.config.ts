import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        ink: "#101820",
        mist: "#f4efe6",
        ember: "#ef6a32",
        pine: "#284b3f",
        tide: "#0d3b66",
        slate: "#4f5d75",
        sand: "#eadfce",
      },
      boxShadow: {
        soft: "0 18px 50px rgba(16, 24, 32, 0.12)",
      },
      backgroundImage: {
        "grid-fade":
          "linear-gradient(rgba(16, 24, 32, 0.06) 1px, transparent 1px), linear-gradient(90deg, rgba(16, 24, 32, 0.06) 1px, transparent 1px)",
      },
      animation: {
        "fade-up": "fade-up 0.55s ease-out both",
        shimmer: "shimmer 1.8s linear infinite",
      },
      keyframes: {
        "fade-up": {
          "0%": {
            opacity: "0",
            transform: "translateY(14px)",
          },
          "100%": {
            opacity: "1",
            transform: "translateY(0)",
          },
        },
        shimmer: {
          "0%": {
            backgroundPosition: "-200% 0",
          },
          "100%": {
            backgroundPosition: "200% 0",
          },
        },
      },
    },
  },
  plugins: [],
};

export default config;
