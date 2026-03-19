import fs from "node:fs/promises";
import path from "node:path";
import sharp from "sharp";

const ROOT = process.cwd();
const OUT_DIR = path.join(ROOT, "public", "icons");
const INPUT_LOGO_CANDIDATES = [
  path.join(ROOT, "public", "logo.png"),
  path.join(ROOT, "public", "assets", "logo.png"),
  path.join(ROOT, "src", "assets", "images", "logo.png"),
];

const BG = "#0F172C";

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function resolveInputLogo() {
  for (const file of INPUT_LOGO_CANDIDATES) {
    try {
      await fs.access(file);
      return file;
    } catch {
      // Try next candidate.
    }
  }

  throw new Error(`Logo de entrada nao encontrada. Caminhos tentados: ${INPUT_LOGO_CANDIDATES.join(", ")}`);
}

async function createIcon({
  inputLogo,
  outFile,
  size,
  scale = 0.78,
  background = BG,
  format = "png",
}) {
  const canvas = sharp({
    create: {
      width: size,
      height: size,
      channels: 4,
      background,
    },
  });

  const logo = await sharp(inputLogo)
    .resize({
      width: Math.round(size * scale),
      height: Math.round(size * scale),
      fit: "inside",
      withoutEnlargement: true,
    })
    .toBuffer();

  await canvas
    .composite([{ input: logo, gravity: "center" }])
    .toFormat(format)
    .toFile(outFile);
}

async function main() {
  await ensureDir(OUT_DIR);
  const inputLogo = await resolveInputLogo();

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "app-icon-192.png"),
    size: 192,
    scale: 0.82,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "app-icon-512.png"),
    size: 512,
    scale: 0.8,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "app-icon.png"),
    size: 512,
    scale: 0.8,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "maskable-192.png"),
    size: 192,
    scale: 0.62,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "maskable-512.png"),
    size: 512,
    scale: 0.62,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "apple-touch-icon.png"),
    size: 180,
    scale: 0.8,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "logo.png"),
    size: 512,
    scale: 0.9,
  });

  await createIcon({
    inputLogo,
    outFile: path.join(OUT_DIR, "logo-bg.png"),
    size: 512,
    scale: 0.9,
  });

  console.log("Icones gerados em public/icons/");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
