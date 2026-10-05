#!/usr/bin/env python3
"""Scrape fuzzor endpoints and aggregate campaign data into a single JSON file.

Each endpoint is expected to serve a directory listing at its root, where
top-level entries are project names (e.g. ``bitcoin``).  Under each project
the scraper looks for ``harnesses/<name>/campaigns/<id>/`` containing
``stats.txt`` and optionally ``coverage-summary.json``.
"""

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.error import URLError

ONE_DAY_SECONDS = 86400
FETCH_TIMEOUT = 10
FETCH_RETRIES = 3


class LinkParser(HTMLParser):
    """Extract href values from directory listing HTML."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value != "../":
                    self.links.append(value.rstrip("/"))


def fetch(url):
    """Fetch URL and return response body as string.

    Retries on timeouts and connection errors but not on HTTP error responses
    (e.g. 404) which indicate the resource genuinely doesn't exist.
    """
    for attempt in range(FETCH_RETRIES):
        try:
            with urlopen(url, timeout=FETCH_TIMEOUT) as resp:
                return resp.read().decode("utf-8")
        except URLError as exc:
            # If the server responded with an HTTP error, don't retry.
            if hasattr(exc, "code"):
                print(f"  HTTP {exc.code} for {url}", file=sys.stderr)
                raise
            # Connection/timeout error — retry if attempts remain.
            if attempt < FETCH_RETRIES - 1:
                print(f"  Retry {attempt + 1}/{FETCH_RETRIES} for {url}: "
                      f"{exc.reason}", file=sys.stderr)
                continue
            print(f"  Failed after {FETCH_RETRIES} attempts for {url}: "
                  f"{exc.reason}", file=sys.stderr)
            raise


def list_dir(url):
    """Parse a directory listing page and return entry names."""
    html = fetch(url)
    parser = LinkParser()
    parser.feed(html)
    return parser.links


def parse_stats(text):
    """Parse stats.txt CSV into list of data points."""
    points = []
    for line in text.strip().splitlines():
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 4:
            continue
        timestamp = parts[0]
        stability_raw = parts[1]
        execs_per_sec = parts[2]
        corpus_count = parts[3]

        stability = None
        if stability_raw.startswith("Some(") and stability_raw.endswith(")"):
            try:
                stability = float(stability_raw[5:-1])
            except ValueError:
                pass

        pt = {
            "timestamp": timestamp,
            "execs_per_sec": int(float(execs_per_sec)),
            "corpus_count": int(float(corpus_count)),
        }
        if stability is not None:
            pt["stability"] = stability
        points.append(pt)
    return points


def parse_coverage(data):
    """Extract coverage totals and per-file data from coverage-summary.json."""
    entry = data["data"][0]
    totals = entry.get("totals", {})
    files = []
    for f in entry.get("files", []):
        if f["summary"]["lines"]["covered"] > 0:
            files.append({
                "filename": f["filename"],
                "lines": f["summary"]["lines"],
                "functions": f["summary"]["functions"],
                "branches": f["summary"]["branches"],
                "regions": f["summary"]["regions"],
            })
    files.sort(key=lambda f: f["lines"]["percent"], reverse=True)
    return {"totals": totals, "files": files}


def campaign_status(campaign_data):
    """Determine whether a campaign is completed, running, or stopped."""
    if campaign_data["coverage"] is not None:
        return "completed"
    if campaign_data["stats"]:
        last_ts = datetime.fromisoformat(
            campaign_data["stats"][-1]["timestamp"]
        )
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - last_ts).total_seconds()
        return "stopped" if age > ONE_DAY_SECONDS else "running"
    return "stopped"


def scrape_campaign(campaign_url, cid):
    """Scrape a single campaign's stats, coverage, and startup params."""
    cdata = {"id": cid, "stats": [], "coverage": None, "startup_params": None}
    try:
        cdata["stats"] = parse_stats(fetch(f"{campaign_url}stats.txt"))
    except URLError:
        pass
    try:
        cov_json = json.loads(fetch(f"{campaign_url}coverage-summary.json"))
        cdata["coverage"] = parse_coverage(cov_json)
    except (URLError, json.JSONDecodeError, KeyError, IndexError):
        pass
    try:
        cdata["startup_params"] = json.loads(
            fetch(f"{campaign_url}startup_params.json"))
    except (URLError, json.JSONDecodeError):
        pass
    cdata["status"] = campaign_status(cdata)
    return cdata


def discover_harness_campaigns(harnesses_url, name):
    """Return (harness_name, [campaign_ids]) for a single harness."""
    harness_url = f"{harnesses_url}{name}/"
    try:
        entries = list_dir(harness_url)
    except URLError:
        return name, []
    if "campaigns" not in entries:
        return name, []
    campaigns_url = f"{harness_url}campaigns/"
    try:
        return name, list_dir(campaigns_url)
    except URLError:
        return name, []


def load_completed_campaigns(previous_dir, project, harness):
    """Return campaigns from a previous run's output that are complete (have
    coverage and stats), keyed by id."""
    if previous_dir is None:
        return {}
    path = os.path.join(previous_dir, "data", project, f"{harness}.json")
    try:
        with open(path) as f:
            campaigns = json.load(f)["campaigns"]
    except (OSError, ValueError, KeyError):
        return {}
    return {c["id"]: c for c in campaigns
            if c.get("coverage") is not None and c.get("stats")}


def scrape_project(base_url, project, pool, previous_dir=None):
    """Scrape all harnesses/campaigns for a single project.

    Completed campaigns found in *previous_dir* are reused instead of scraped.
    """
    harnesses_url = f"{base_url}/{project}/harnesses/"
    try:
        harness_names = list_dir(harnesses_url)
    except URLError:
        return {}

    # Phase 1: discover campaign IDs for all harnesses in parallel
    discovery_futures = [
        pool.submit(discover_harness_campaigns, harnesses_url, n)
        for n in harness_names
    ]
    harness_cids = {}
    for fut in discovery_futures:
        name, cids = fut.result()
        harness_cids[name] = cids

    # Phase 2: scrape new and unfinished campaigns in parallel, reuse
    # completed ones from the previous run
    reused = {}
    campaign_futures = {}
    for name, cids in harness_cids.items():
        campaigns_url = f"{harnesses_url}{name}/campaigns/"
        completed = load_completed_campaigns(previous_dir, project, name)
        reused[name] = []
        futs = []
        for cid in cids:
            if cid in completed:
                reused[name].append(completed[cid])
            else:
                futs.append(
                    pool.submit(scrape_campaign, f"{campaigns_url}{cid}/", cid)
                )
        campaign_futures[name] = futs

    # Collect results
    result = {}
    for name in harness_names:
        campaigns = reused.get(name, []) + [
            f.result() for f in campaign_futures.get(name, [])
        ]
        campaigns.sort(
            key=lambda c: c["stats"][0]["timestamp"] if c["stats"] else ""
        )
        result[name] = {"campaigns": campaigns}
        total_pts = sum(len(c["stats"]) for c in campaigns)
        print(f"    {name}: {len(campaigns)} campaign(s), "
              f"{len(reused.get(name, []))} reused, "
              f"{total_pts} data points", file=sys.stderr)

    return result


def scrape_endpoint(endpoint, only_projects=None, pool=None,
                    previous_dir=None):
    """Discover projects at *endpoint* and scrape each one.

    If *only_projects* is given, only scrape projects whose names are in the set.
    """
    endpoint = endpoint.rstrip("/")
    projects = {}

    try:
        top_level = list_dir(endpoint)
    except URLError as exc:
        print(f"  Failed to list {endpoint}: {exc}", file=sys.stderr)
        return projects

    for entry in top_level:
        if only_projects is not None and entry not in only_projects:
            continue
        # Each top-level directory is a project
        print(f"  Project: {entry}", file=sys.stderr)
        harness_data = scrape_project(endpoint, entry, pool, previous_dir)
        if entry in projects:
            projects[entry]["harnesses"].update(harness_data)
        else:
            projects[entry] = {"harnesses": harness_data}

    return projects


def main():
    parser = argparse.ArgumentParser(
        description="Scrape fuzzor endpoints and aggregate campaign data."
    )
    parser.add_argument(
        "endpoints", nargs="+",
        help="One or more base URLs to scrape (e.g. http://host:8888)",
    )
    parser.add_argument(
        "-o", "--output", default="data.json",
        help="Output JSON file (default: data.json)",
    )
    parser.add_argument(
        "-p", "--projects", nargs="+", metavar="NAME",
        help="Only scrape these projects (default: all discovered projects)",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=4,
        help="Number of parallel fetch threads (default: 4)",
    )
    parser.add_argument(
        "--previous", metavar="DIR",
        help="Output directory of a previous run; completed campaigns found "
             "there are reused instead of scraped again",
    )
    args = parser.parse_args()

    previous_dir = args.previous
    if previous_dir and not os.path.isdir(os.path.join(previous_dir, "data")):
        print(f"No previous data in {previous_dir}, scraping everything",
              file=sys.stderr)
        previous_dir = None

    all_projects = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for endpoint in args.endpoints:
            print(f"Scraping {endpoint}", file=sys.stderr)
            only = set(args.projects) if args.projects else None
            projects = scrape_endpoint(endpoint, only_projects=only,
                                       pool=pool, previous_dir=previous_dir)
            for pname, pdata in projects.items():
                if pname in all_projects:
                    all_projects[pname]["harnesses"].update(
                        pdata["harnesses"])
                else:
                    all_projects[pname] = pdata

    scraped_at = datetime.now(timezone.utc).isoformat()

    # Write per-harness detail files and build summary index
    output_dir = os.path.dirname(args.output) or "."
    data_dir = os.path.join(output_dir, "data")

    index_projects = {}
    for pname, pdata in all_projects.items():
        project_data_dir = os.path.join(data_dir, pname)
        os.makedirs(project_data_dir, exist_ok=True)

        index_harnesses = {}
        for hname, hdata in pdata["harnesses"].items():
            # Write full campaign data to per-harness detail file
            detail_path = os.path.join(project_data_dir, f"{hname}.json")
            with open(detail_path, "w") as f:
                json.dump({"campaigns": hdata["campaigns"]}, f)

            # Build lightweight summary for the index
            summary_campaigns = []
            for c in hdata["campaigns"]:
                avg_execs = None
                avg_stability = None
                if c["stats"]:
                    avg_execs = sum(s["execs_per_sec"] for s in c["stats"]) / len(c["stats"])
                    stab_vals = [s["stability"] for s in c["stats"] if "stability" in s and s["stability"] is not None]
                    if stab_vals:
                        avg_stability = sum(stab_vals) / len(stab_vals)
                sp = c.get("startup_params")
                summary = {
                    "id": c["id"],
                    "status": c["status"],
                    "first_ts": (c["stats"][0]["timestamp"]
                                 if c["stats"] else None),
                    "last_ts": (c["stats"][-1]["timestamp"]
                                if c["stats"] else None),
                    "stats_count": len(c["stats"]),
                    "coverage_totals": (c["coverage"]["totals"]
                                        if c["coverage"] else None),
                    "avg_execs_per_sec": avg_execs,
                    "avg_stability": avg_stability,
                    "commit_hash": sp.get("commit_hash") if sp else None,
                    "num_cpus": sp.get("num_cpus") if sp else None,
                    "duration_secs": sp.get("duration_secs") if sp else None,
                }
                summary_campaigns.append(summary)
            index_harnesses[hname] = {"campaigns": summary_campaigns}

        index_projects[pname] = {"harnesses": index_harnesses}

    with open(args.output, "w") as f:
        json.dump({"scraped_at": scraped_at, "projects": index_projects}, f)

    total_harnesses = sum(
        len(p["harnesses"]) for p in all_projects.values()
    )
    print(f"Wrote {args.output} + {data_dir}/ ({len(all_projects)} projects, "
          f"{total_harnesses} harnesses)", file=sys.stderr)


if __name__ == "__main__":
    main()
