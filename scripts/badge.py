#!/usr/bin/env python3
"""Transform coverage/coverage.json into a shields.io endpoint badge JSON.

Usage: badge.py <coverage.json> <out.json>

The README badge points at the committed out.json via the shields endpoint:
https://img.shields.io/endpoint?url=<raw-url-of-out.json>
"""
import json
import sys


def color(pct):
    if pct >= 90:
        return "brightgreen"
    if pct >= 80:
        return "green"
    if pct >= 70:
        return "yellowgreen"
    if pct >= 60:
        return "yellow"
    if pct >= 50:
        return "orange"
    return "red"


def main():
    src, dst = sys.argv[1], sys.argv[2]
    with open(src) as fh:
        total = float(json.load(fh)["total"])
    badge = {
        "schemaVersion": 1,
        "label": "coverage",
        "message": f"{total:.1f}%",
        "color": color(total),
    }
    with open(dst, "w") as fh:
        json.dump(badge, fh)
        fh.write("\n")
    print(f"badge: {badge['message']} ({badge['color']})")


if __name__ == "__main__":
    main()
