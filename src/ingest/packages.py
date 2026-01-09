from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import httpx
from subsets_utils import get, save_raw_json, load_state, save_state

SEARCH_URL = "https://registry.npmjs.org/-/v1/search"
TARGET_COUNT = 10000
PAGE_SIZE = 250


def should_retry(exception):
    """Only retry transient errors."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in (429, 500, 502, 503, 504)
    if isinstance(exception, httpx.RequestError):
        return True
    return False


@sleep_and_retry
@limits(calls=5, period=60)  # 5 calls per minute to be very conservative
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception(should_retry),
    reraise=True
)
def rate_limited_get(url, params):
    """Get with rate limiting and retry logic."""
    response = get(url, params=params)
    response.raise_for_status()
    return response


def run():
    """Fetch top packages from npm registry using search API.

    Uses broad search terms to get enough packages sorted by popularity.
    The npm search API requires a text query, so we use common terms that
    match most packages ("npm", "node", "package") and deduplicate results.
    """
    state = load_state("npm_packages")
    all_packages = state.get("packages", [])
    seen_names = set(state.get("seen_names", []))

    if len(all_packages) >= TARGET_COUNT:
        print(f"Already have {len(all_packages)} packages cached")
        save_raw_json(all_packages, "popular_packages")
        return

    print(f"Fetching top {TARGET_COUNT} npm packages...")
    print(f"Starting with {len(all_packages)} existing packages")

    # Search terms ordered by result count - "npm" returns 1.6M+ packages
    search_terms = ["npm", "node", "package"]

    for search_term in search_terms:
        if len(all_packages) >= TARGET_COUNT:
            break

        offset = 0
        consecutive_no_new = 0
        print(f"  Searching with term: '{search_term}'")

        while len(all_packages) < TARGET_COUNT:
            params = {
                "text": search_term,
                "size": PAGE_SIZE,
                "from": offset,
                "quality": "0.0",
                "popularity": "1.0",
                "maintenance": "0.0",
            }

            response = rate_limited_get(SEARCH_URL, params=params)
            data = response.json()

            if not data.get("objects"):
                print(f"    No more results at offset {offset}")
                break

            new_count = 0
            for obj in data["objects"]:
                pkg = obj.get("package", {})
                name = pkg.get("name")
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                new_count += 1
                score = obj.get("score", {})
                all_packages.append({
                    "name": name,
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
            save_state("npm_packages", {"packages": all_packages, "seen_names": list(seen_names)})
            print(f"    Fetched {len(all_packages):,} packages (+{new_count} new, offset: {offset})")

            # If we're getting mostly duplicates, move to next search term
            if new_count == 0:
                consecutive_no_new += 1
                if consecutive_no_new >= 3:
                    print(f"    Stopping '{search_term}' - too many duplicates")
                    break
            else:
                consecutive_no_new = 0

    print(f"Total: {len(all_packages):,} packages")
    save_raw_json(all_packages, "popular_packages")
