import argparse

from subsets_utils import validate_environment

from ingest import packages as ingest_packages

from transforms.popular_packages import main as transform_popular_packages


TRANSFORMS = [
    ("popular_packages", transform_popular_packages),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n" + "=" * 50)
        print("Phase 1: Ingest")
        print("=" * 50)
        print("\n--- Fetching npm packages ---")
        ingest_packages.run()

    if should_transform:
        print("\n" + "=" * 50)
        print("Phase 2: Transform")
        print("=" * 50)
        for name, transform_module in TRANSFORMS:
            print(f"\n--- Transforming {name} ---")
            transform_module.run()

    print("\n" + "=" * 50)
    print("npm-registry complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
