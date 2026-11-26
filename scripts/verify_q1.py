"""Prepare a small converter input or verify two Q1 runner directories."""

import argparse
import json
from collections import Counter, defaultdict
from math import isclose
from pathlib import Path


def create_fixture(root):
    samples = [
        (2015, "é" * 249, 2, 1),
        (2015, "x" * 250, 4, 3),
        (2015, "x" * 749, 2, 5),
        (2021, "x" * 750, 3, 2),
        (2021, "x" * 1999, 5, 8),
        (2021, "x" * 2000, 5, 9),
        (2021, "", 4, 3),
        (2014, "early", 1, 10),
        (2022, "late", 1, 10),
        (2015, "zero votes", 1, 0),
        (2021, None, 1, 10),
    ]
    raw = root / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    with (raw / "yelp_academic_dataset_review.json").open("x", encoding="utf-8") as output:
        for index, (year, text, stars, useful) in enumerate(samples):
            review = dict(review_id=str(index), user_id="u", business_id="b",
                          stars=stars, useful=useful, funny=0, cool=0, text=text,
                          date=f"{year}-01-01 00:00:00")
            output.write(json.dumps(review) + "\n")


def check_rows(actual, expected):
    assert len(actual) == len(expected), (actual, expected)
    for row, reference in zip(actual, expected):
        assert row.keys() == reference.keys(), (row, reference)
        for key, value in reference.items():
            agrees = isclose(row[key], value, rel_tol=1e-12, abs_tol=1e-12) if isinstance(value, float) else row[key] == value
            assert agrees, (key, row, reference)


def verify(dataframe, sql, fixture):
    def read(directory, name, kind="rows"):
        return json.loads((directory / f"{name}.{kind}.json").read_text(encoding="utf-8"))

    part_a, part_b = "q1-a", "q1-b"
    for api, directory in (("dataframe", dataframe), ("sql", sql)):
        app_ids = set()
        for name in (part_a, part_b):
            metrics = read(directory, name, "metrics")
            assert metrics["api"] == api and metrics["application_id"], metrics
            assert metrics["row_count"] == len(read(directory, name)), metrics
            app_ids.add(metrics["application_id"])
            assert "Physical Plan" in (directory / f"{name}.plan.txt").read_text(encoding="utf-8")
        assert len(app_ids) == 1, app_ids
        print(f"{api} {app_ids.pop()} {directory}")

    a, b = read(dataframe, part_a), read(dataframe, part_b)
    check_rows(a, read(sql, part_a))
    check_rows(b, read(sql, part_b))
    audit = read(dataframe, "q1-eligibility")
    assert read(dataframe, "q1-eligibility", "metrics")["application_id"] == read(dataframe, part_a, "metrics")["application_id"]
    totals = {row["year"]: row["eligible_count"] - row["null_text_count"] for row in audit}
    assert set(totals) == ({2015, 2021} if fixture else set(range(2015, 2022))), totals
    percentages = defaultdict(float)
    categorized = Counter()
    pairs = set()
    for row in b:
        year, category, percentage = row["year"], row["category"], row["percentage"]
        assert (year, category) not in pairs, row
        pairs.add((year, category))
        assert 0 < percentage <= 100, row
        percentages[year] += percentage
        count = totals[year] * percentage / 100
        assert isclose(count, round(count), abs_tol=1e-8), row
        categorized[category] += round(count)
    assert percentages.keys() == {year for year, count in totals.items() if count}, percentages
    assert all(isclose(value, 100, abs_tol=1e-9) for value in percentages.values()), percentages
    assert len(a) == len(categorized), a
    assert categorized == {row["category"]: row["review_count"] for row in a}, categorized
    assert sum(categorized.values()) == sum(totals.values()), categorized
    assert a == sorted(a, key=lambda row: (-row["avg_useful"], row["category"])), a
    if fixture:
        check_rows(a, [
            dict(category="Very Long", review_count=1, avg_stars=5.0, avg_useful=9.0),
            dict(category="Long", review_count=2, avg_stars=4.0, avg_useful=5.0),
            dict(category="Medium", review_count=2, avg_stars=3.0, avg_useful=4.0),
            dict(category="Short", review_count=2, avg_stars=3.0, avg_useful=2.0),
        ])
        check_rows(b, [
            dict(year=2015, category="Medium", percentage=200 / 3),
            dict(year=2015, category="Short", percentage=100 / 3),
            dict(year=2021, category="Long", percentage=50.0),
            dict(year=2021, category="Short", percentage=25.0),
            dict(year=2021, category="Very Long", percentage=25.0),
        ])
        assert audit == [
            dict(year=2015, eligible_count=3, null_text_count=0),
            dict(year=2021, eligible_count=5, null_text_count=1),
        ], audit
    print("Category totals", dict(categorized))
    print("Yearly percentage sums", dict(percentages))
    print("Eligibility audit", audit)
    print("Q1 verification passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fixture = commands.add_parser("fixture")
    fixture.add_argument("data_dir", type=Path)
    check = commands.add_parser("check")
    check.add_argument("dataframe_dir", type=Path)
    check.add_argument("sql_dir", type=Path)
    check.add_argument("--fixture", action="store_true")
    args = parser.parse_args()
    if args.command == "fixture":
        create_fixture(args.data_dir)
    else:
        verify(args.dataframe_dir, args.sql_dir, args.fixture)


if __name__ == "__main__":
    main()
