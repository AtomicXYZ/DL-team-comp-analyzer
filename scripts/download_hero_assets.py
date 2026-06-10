from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


DEFAULT_HEROES_URL = "https://api.deadlock-api.com/v1/assets/heroes"
DEFAULT_OUTPUT_DIR = Path("app/assets/heroes")
DEFAULT_MANIFEST = Path("app/assets/hero_images.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Deadlock hero portraits for the Streamlit app.")
    parser.add_argument("--heroes-url", default=DEFAULT_HEROES_URL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--image-kind", default="icon_hero_card_webp")
    parser.add_argument("--fallback-image-kind", default="icon_image_small_webp")
    parser.add_argument("--hero-ids", nargs="*", help="Optional hero IDs to download. Defaults to all heroes.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    heroes = fetch_json(args.heroes_url)
    allowed_ids = set(args.hero_ids or [])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, dict[str, str]] = {}
    for hero in heroes:
        hero_id = str(hero.get("id", "")).strip()
        if not hero_id or allowed_ids and hero_id not in allowed_ids:
            continue

        images = hero.get("images") or {}
        image_url = images.get(args.image_kind) or images.get(args.fallback_image_kind)
        if not image_url:
            continue

        suffix = Path(image_url).suffix or ".webp"
        image_path = args.output_dir / f"{hero_id}{suffix}"
        download_file(image_url, image_path)
        manifest[hero_id] = {
            "name": str(hero.get("name") or f"Hero {hero_id}"),
            "image": str(image_path.relative_to(args.manifest.parent)),
            "source": image_url,
        }
        print(f"{hero_id}: {manifest[hero_id]['name']} -> {image_path}")

    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote manifest for {len(manifest)} heroes to {args.manifest}")
    return 0


def fetch_json(url: str) -> list[dict]:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "dl-team-comp-analyzer/0.1"})
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected a list from {url}")
    return payload


def download_file(url: str, path: Path) -> None:
    request = Request(url, headers={"User-Agent": "dl-team-comp-analyzer/0.1"})
    with urlopen(request, timeout=30) as response:
        path.write_bytes(response.read())


if __name__ == "__main__":
    raise SystemExit(main())
