import time
from subsets_utils import get, save_raw_json, load_state, save_state

SEARCH_URL = "https://registry.npmjs.org/-/v1/search"
TARGET_COUNT = 10000
PAGE_SIZE = 250


def run():
    """Fetch top packages from npm registry using search API."""
    state = load_state("npm_packages")
    all_packages = state.get("packages", [])
    offset = state.get("offset", 0)

    if len(all_packages) >= TARGET_COUNT:
        print(f"Already have {len(all_packages)} packages cached")
        save_raw_json(all_packages, "popular_packages")
        return

    print(f"Fetching top {TARGET_COUNT} npm packages...")
    print(f"Starting from offset {offset} with {len(all_packages)} existing")

    while len(all_packages) < TARGET_COUNT:
        params = {
            "text": "boost-exact:false",
            "size": PAGE_SIZE,
            "from": offset,
            "quality": "0.0",
            "popularity": "1.0",
            "maintenance": "0.0",
        }

        response = get(SEARCH_URL, params=params)
        data = response.json()

        if not data.get("objects"):
            print(f"No more results at offset {offset}")
            break

        for obj in data["objects"]:
            pkg = obj.get("package", {})
            score = obj.get("score", {})
            all_packages.append({
                "name": pkg.get("name"),
                "version": pkg.get("version"),
                "description": pkg.get("description"),
                "license": pkg.get("license"),
                "date": pkg.get("date"),
                "publisher_username": pkg.get("publisher", {}).get("username"),
                "maintainers_count": len(pkg.get("maintainers", [])),
                "keywords": pkg.get("keywords"),
                "repository_url": pkg.get("links", {}).get("repository"),
                "homepage_url": pkg.get("links", {}).get("homepage"),
                "npm_url": pkg.get("links", {}).get("npm"),
                "score_final": score.get("final"),
                "score_quality": score.get("detail", {}).get("quality"),
                "score_popularity": score.get("detail", {}).get("popularity"),
                "score_maintenance": score.get("detail", {}).get("maintenance"),
            })

        offset += PAGE_SIZE
        save_state("npm_packages", {"packages": all_packages, "offset": offset})
        print(f"  Fetched {len(all_packages):,} packages (offset: {offset})")

        time.sleep(0.5)

    print(f"Total: {len(all_packages):,} packages")
    save_raw_json(all_packages, "popular_packages")
