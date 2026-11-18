#!/usr/bin/env python3
"""Render 1024x1024 pgflight dock icon (Pillow)."""
from pathlib import Path

from PIL import Image, ImageDraw

SIZE = 1024
OUT = Path(__file__).resolve().parent.parent / "src-tauri" / "icons" / "icon.png"


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def main() -> None:
    margin = 48
    radius = 192
    box = (margin, margin, SIZE - margin, SIZE - margin)

    img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    grad = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    gd = ImageDraw.Draw(grad)
    for y in range(margin, SIZE - margin):
        t = (y - margin) / (SIZE - 2 * margin)
        gd.line(
            [(margin, y), (SIZE - margin, y)],
            fill=(
                int(lerp(29, 74, t)),
                int(lerp(78, 158, t)),
                int(lerp(216, 255, t)),
                255,
            ),
        )

    mask = Image.new("L", (SIZE, SIZE), 0)
    ImageDraw.Draw(mask).rounded_rectangle(box, radius=radius, fill=255)
    grad.putalpha(mask)
    img = grad.copy()
    d = ImageDraw.Draw(img)

    scale = SIZE / 32.0
    ox = margin + (SIZE - 2 * margin - 26 * scale) / 2
    oy = margin + (SIZE - 2 * margin - 22 * scale) / 2 - 40

    def sx(v: float) -> float:
        return ox + v * scale

    def sy(v: float) -> float:
        return oy + v * scale

    body = [
        (sx(6), sy(18.5)),
        (sx(10.2), sy(18.5)),
        (sx(11.6), sy(16.4)),
        (sx(16.8), sy(14.8)),
        (sx(19.6), sy(15.9)),
        (sx(25.2), sy(13.7)),
        (sx(26.4), sy(14.6)),
        (sx(23), sy(17.4)),
        (sx(20.6), sy(16.8)),
        (sx(16.8), sy(19.7)),
        (sx(9.4), sy(19.7)),
        (sx(6), sy(20.2)),
    ]
    d.polygon(body, fill=(232, 238, 246, 245))

    tape_y = sy(22)
    lw = max(6, int(1.5 * scale))
    d.line([(sx(7), tape_y), (sx(25), tape_y)], fill=(74, 158, 255, 255), width=lw)

    blip_x, blip_y = sx(23), tape_y
    r = max(10, int(2.2 * scale))
    d.ellipse((blip_x - r, blip_y - r, blip_x + r, blip_y + r), fill=(52, 211, 153, 255))

    d.rounded_rectangle(
        (margin + 6, margin + 6, SIZE - margin - 6, SIZE - margin - 6),
        radius=radius - 6,
        outline=(255, 255, 255, 40),
        width=5,
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    img.save(OUT, "PNG", optimize=True)
    print(f"wrote {OUT} ({SIZE}x{SIZE})")


if __name__ == "__main__":
    main()