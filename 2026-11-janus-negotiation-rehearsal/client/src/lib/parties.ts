/**
 * Party identity colours, in fixed order (never cycled by rank): Databricks ramps,
 * checked with the dataviz validator against white. Lightness, chroma, contrast and
 * normal-vision separation pass; Green/Yellow sit at ΔE 6.9 under protanopia, so every
 * chart that uses them also labels its lines directly.
 *   line: the mark on charts
 *   ink:  the 800 step, for an initial or label on the tint
 *   tint: the brand's flat 12% tint, for the identity disc
 */
export interface PartyHue {
  line: string;
  ink: string;
  tint: string;
}

const HUES: PartyHue[] = [
  { line: "#2272B4", ink: "#04355D", tint: "#E4EEF6" }, // Blue 600
  { line: "#BA7B23", ink: "#7D5319", tint: "#FFF5E0" }, // Yellow 700
  { line: "#00875C", ink: "#095A35", tint: "#E0F5EE" }, // Green 700
  { line: "#FF3621", ink: "#801C17", tint: "#FFE7E4" }, // Lava 600
  { line: "#AB4057", ink: "#4A121A", tint: "#F3E2E5" }, // Maroon 500
  { line: "#4299E0", ink: "#0E538B", tint: "#E4EEF6" }, // Blue 500
];

export function partyHue(index: number): PartyHue {
  return HUES[index % HUES.length];
}
