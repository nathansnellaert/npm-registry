import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_positive, assert_in_range


def test(table: pa.Table) -> None:
    """Validate npm_popular_packages output."""
    validate(table, {
        "columns": {
            "name": "string",
            "version": "string",
            "description": "string",
            "license": "string",
            "date": "string",
            "publisher_username": "string",
            "maintainers_count": "int",
            "keywords": "list",
            "repository_url": "string",
            "homepage_url": "string",
            "npm_url": "string",
            "score_final": "double",
            "score_quality": "double",
            "score_popularity": "double",
            "score_maintenance": "double",
        },
        "not_null": ["name"],
        "unique": ["name"],
        "min_rows": 5000,
    })

    assert_positive(table, "maintainers_count", allow_zero=True)

    names = table.column("name").to_pylist()[:100]
    assert all(n and len(n) > 0 for n in names), "Package names should be non-empty"
