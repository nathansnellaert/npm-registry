import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "npm_popular_packages"

METADATA = {
    "id": DATASET_ID,
    "title": "npm Popular Packages",
    "description": "Top 10,000 most popular packages on npm (Node.js package registry), ranked by popularity score. Includes metadata, licensing, and quality scores.",
    "column_descriptions": {
        "name": "Package name (used in package.json)",
        "version": "Latest published version",
        "description": "Short description of the package",
        "license": "License identifier (e.g., MIT, Apache-2.0)",
        "date": "Timestamp of latest publish",
        "publisher_username": "npm username of latest publisher",
        "maintainers_count": "Number of maintainers",
        "keywords": "List of keywords/tags",
        "repository_url": "Source code repository URL",
        "homepage_url": "Project homepage URL",
        "npm_url": "npm package page URL",
        "score_final": "Overall npm score (0-1)",
        "score_quality": "Quality score component (0-1)",
        "score_popularity": "Popularity score component (0-1)",
        "score_maintenance": "Maintenance score component (0-1)",
    }
}


def run():
    """Transform npm packages into npm_popular_packages dataset."""
    raw = load_raw_json("popular_packages")
    print(f"  Loaded {len(raw):,} raw packages")

    records = []
    for pkg in raw:
        if not pkg.get("name"):
            continue
        records.append({
            "name": pkg["name"],
            "version": pkg.get("version"),
            "description": pkg.get("description") if pkg.get("description") else None,
            "license": pkg.get("license") if pkg.get("license") else None,
            "date": pkg.get("date") if pkg.get("date") else None,
            "publisher_username": pkg.get("publisher_username") if pkg.get("publisher_username") else None,
            "maintainers_count": pkg.get("maintainers_count", 0),
            "keywords": pkg.get("keywords") if pkg.get("keywords") else None,
            "repository_url": pkg.get("repository_url") if pkg.get("repository_url") else None,
            "homepage_url": pkg.get("homepage_url") if pkg.get("homepage_url") else None,
            "npm_url": pkg.get("npm_url") if pkg.get("npm_url") else None,
            "score_final": pkg.get("score_final"),
            "score_quality": pkg.get("score_quality"),
            "score_popularity": pkg.get("score_popularity"),
            "score_maintenance": pkg.get("score_maintenance"),
        })

    print(f"  Transformed {len(records):,} packages")
    table = pa.Table.from_pylist(records)

    test(table)

    upload_data(table, DATASET_ID, METADATA, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
