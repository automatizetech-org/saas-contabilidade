import fs from "node:fs/promises";
import path from "node:path";
import sharp from "sharp";

const ROOT = process.cwd();
const INPUT_LOGO = path.join(ROOT, "public", "logo.png");
const OUT_DIR = path.join(ROOT, "public", "icons");

// Tom azul escuro do app (mantido consistente com theme_color/background_color do manifest)
const BG = "#0F172C";

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function createIcon({
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

  const logo = await sharp(INPUT_LOGO)
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

  // Ícones "any" (Android/Chrome)
  await createIcon({
    outFile: path.join(OUT_DIR, "app-icon-192.png"),
    size: 192,
    scale: 0.82,
  });
  await createIcon({
    outFile: path.join(OUT_DIR, "app-icon-512.png"),
    size: 512,
    scale: 0.80,
  });

  // Compatibilidade com referência antiga (se sobrar algum link em cache)
  await createIcon({
    outFile: path.join(OUT_DIR, "app-icon.png"),
    size: 512,
    scale: 0.80,
  });

  // Maskable: precisa mais “respiro” para não cortar em ícones arredondados
  await createIcon({
    outFile: path.join(OUT_DIR, "maskable-192.png"),
    size: 192,
    scale: 0.62,
  });
  await createIcon({
    outFile: path.join(OUT_DIR, "maskable-512.png"),
    size: 512,
    scale: 0.62,
  });

  // iOS
  await createIcon({
    outFile: path.join(OUT_DIR, "apple-touch-icon.png"),
    size: 180,
    scale: 0.80,
  });

  // Cópia da logo dentro de /icons, mas com fundo (sem transparência)
  // Mantém no diretório /icons (não afeta o /public/logo.png usado no site).
  await createIcon({
    outFile: path.join(OUT_DIR, "logo.png"),
    size: 512,
    scale: 0.90,
  });

  // Alias opcional (caso você já tenha referenciado esse nome em algum lugar)
  await createIcon({
    outFile: path.join(OUT_DIR, "logo-bg.png"),
    size: 512,
    scale: 0.90,
  });

  // eslint-disable-next-line no-console
  console.log("Ícones gerados em public/icons/");
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exitCode = 1;
});
