#!/usr/bin/env python3
"""Normalize the raw Paris 2024 datasets into Neo4j-friendly CSVs."""

import ast
import csv
import re
import shutil
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
IMPORT_DIR = BASE_DIR / "import"
RESULTS_DIR = IMPORT_DIR / "results"
OUT_DIR = IMPORT_DIR / "normalized"

SLUG_PATTERN = re.compile(r"[^A-Za-z0-9]+")


def slugify(value: str) -> str:
    """Convert free-form text into a slug that can be used for identifiers."""
    if not value:
        return ""
    return SLUG_PATTERN.sub("-", value.strip().lower()).strip("-")


def parse_list(value: str) -> list[str]:
    """Parse columns that are stored as serialized Python lists."""
    if not value:
        return []
    value = value.strip()
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
    except Exception:
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(parsed, (list, tuple, set)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    if parsed:
        return [str(parsed).strip()]
    return []


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]):
    """Persist rows to CSV with a deterministic column order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_countries():
    countries_path = IMPORT_DIR / "nocs.csv"
    records = []
    countries = {}
    with countries_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["code"].strip()
            countries[code] = row
            records.append(
                {
                    "country_code:ID(Country-ID)": code,
                    "name": row.get("country", "").strip(),
                    "name_long": row.get("country_long", "").strip(),
                    "tag": row.get("tag", "").strip(),
                    "note": row.get("note", "").strip(),
                }
            )
    records.sort(key=lambda item: item["country_code:ID(Country-ID)"])
    write_csv(
        OUT_DIR / "nodes_countries.csv",
        ["country_code:ID(Country-ID)", "name", "name_long", "tag", "note"],
        records,
    )
    return countries


def build_sports():
    events_path = IMPORT_DIR / "events.csv"
    schedules_path = IMPORT_DIR / "schedules.csv"
    sports = {}

    def ensure(code: str, name: str = "", tag: str = "", url: str = ""):
        if not code:
            return
        entry = sports.setdefault(
            code,
            {
                "sport_code:ID(Sport-ID)": code,
                "name": "",
                "tag": "",
                "url": "",
            },
        )
        if name and not entry["name"]:
            entry["name"] = name
        if tag and not entry["tag"]:
            entry["tag"] = tag
        if url and not entry["url"]:
            entry["url"] = url

    with events_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ensure(
                row.get("sport_code", "").strip(),
                row.get("sport", "").strip(),
                row.get("tag", "").strip(),
                row.get("sport_url", "").strip(),
            )

    with schedules_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ensure(row.get("discipline_code", "").strip(), row.get("discipline", "").strip())

    records = sorted(sports.values(), key=lambda item: item["sport_code:ID(Sport-ID)"])
    write_csv(OUT_DIR / "nodes_sports.csv", ["sport_code:ID(Sport-ID)", "name", "tag", "url"], records)
    return sports


def build_venues_and_sessions():
    venues_path = IMPORT_DIR / "venues.csv"
    schedules_path = IMPORT_DIR / "schedules.csv"
    venues_by_name = {}
    with venues_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            venues_by_name[row["venue"].strip()] = row

    venues = {}
    sessions = []
    sport_venue_pairs = set()
    session_sport = []
    session_venue = []

    unknown_counter = 0
    with schedules_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            venue_name = row.get("venue", "").strip()
            venue_code = row.get("venue_code", "").strip() or row.get("location_code", "").strip()
            if not venue_code:
                venue_code = slugify(venue_name or row.get("location_description", ""))
            if not venue_code:
                venue_code = f"anon-{unknown_counter}"
                unknown_counter += 1
            discipline_code = row.get("discipline_code", "").strip()
            sport_venue_pairs.add((discipline_code, venue_code))

            venue_entry = venues.setdefault(
                venue_code,
                {
                    "venue_code:ID(Venue-ID)": venue_code,
                    "name": venue_name,
                    "location": row.get("location_description", "").strip(),
                    "tag": "",
                    "url": "",
                    "sports_hosted": set(),
                },
            )
            if discipline_code:
                venue_entry["sports_hosted"].add(discipline_code)
            venue_details = venues_by_name.get(venue_name)
            if venue_details:
                venue_entry["tag"] = venue_entry["tag"] or venue_details.get("tag", "").strip()
                venue_entry["url"] = venue_entry["url"] or venue_details.get("url", "").strip()

            session_id = row.get("url", "").strip()
            if not session_id:
                session_id = f"{discipline_code}_{slugify(row.get('event', ''))}_{row.get('start_date', '')}"
            session_id = slugify(session_id)
            if not session_id:
                continue
            sessions.append(
                {
                    "session_id:ID(Session-ID)": session_id,
                    "start_datetime": row.get("start_date", "").strip(),
                    "end_datetime": row.get("end_date", "").strip(),
                    "day": row.get("day", "").strip(),
                    "status": row.get("status", "").strip(),
                    "sport": row.get("discipline", "").strip(),
                    "sport_code": discipline_code,
                    "event_phase": row.get("phase", "").strip(),
                    "event_name": row.get("event", "").strip(),
                    "gender": row.get("gender", "").strip(),
                    "event_type": row.get("event_type", "").strip(),
                    "medal_session": row.get("event_medal", "").strip(),
                    "venue_code": venue_code,
                }
            )
            if discipline_code:
                session_sport.append(
                    {
                        ":START_ID(Session-ID)": session_id,
                        ":END_ID(Sport-ID)": discipline_code,
                        ":TYPE": "SESSION_FOR",
                    }
                )
            session_venue.append(
                {
                    ":START_ID(Session-ID)": session_id,
                    ":END_ID(Venue-ID)": venue_code,
                    ":TYPE": "HELD_AT",
                }
            )

    venue_records = []
    for venue_code, entry in sorted(venues.items()):
        sports_hosted = ";".join(sorted(code for code in entry["sports_hosted"] if code))
        venue_records.append(
            {
                "venue_code:ID(Venue-ID)": entry["venue_code:ID(Venue-ID)"],
                "name": entry["name"],
                "location": entry["location"],
                "tag": entry["tag"],
                "url": entry["url"],
                "sport_codes": sports_hosted,
            }
        )

    write_csv(
        OUT_DIR / "nodes_venues.csv",
        ["venue_code:ID(Venue-ID)", "name", "location", "tag", "url", "sport_codes"],
        venue_records,
    )

    sessions.sort(key=lambda item: item["session_id:ID(Session-ID)"])
    write_csv(
        OUT_DIR / "nodes_sessions.csv",
        [
            "session_id:ID(Session-ID)",
            "start_datetime",
            "end_datetime",
            "day",
            "status",
            "sport",
            "sport_code",
            "event_phase",
            "event_name",
            "gender",
            "event_type",
            "medal_session",
            "venue_code",
        ],
        sessions,
    )

    write_csv(
        OUT_DIR / "rels_session_sport.csv",
        [":START_ID(Session-ID)", ":END_ID(Sport-ID)", ":TYPE"],
        sorted(session_sport, key=lambda item: (item[":START_ID(Session-ID)"], item[":END_ID(Sport-ID)"])),
    )

    write_csv(
        OUT_DIR / "rels_session_venue.csv",
        [":START_ID(Session-ID)", ":END_ID(Venue-ID)", ":TYPE"],
        sorted(session_venue, key=lambda item: (item[":START_ID(Session-ID)"], item[":END_ID(Venue-ID)"])),
    )

    sport_venue = [
        {
            ":START_ID(Sport-ID)": sport,
            ":END_ID(Venue-ID)": venue,
            ":TYPE": "USE_VENUE",
        }
        for sport, venue in sorted({pair for pair in sport_venue_pairs if pair[0] and pair[1]})
    ]
    write_csv(
        OUT_DIR / "rels_sport_venue.csv",
        [":START_ID(Sport-ID)", ":END_ID(Venue-ID)", ":TYPE"],
        sport_venue,
    )

    venue_name_lookup = {}
    for venue_code, entry in venues.items():
        if entry["name"]:
            venue_name_lookup[entry["name"]] = venue_code
    return venue_name_lookup


def build_people(valid_countries: dict[str, dict]):
    athletes_path = IMPORT_DIR / "athletes.csv"
    coaches_path = IMPORT_DIR / "coaches.csv"
    officials_path = IMPORT_DIR / "technical_officials.csv"

    athlete_rows = []
    athlete_country = []
    with athletes_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["code"].strip()
            athlete_rows.append(
                {
                    "athlete_code:ID(Athlete-ID)": code,
                    "name": row.get("name", "").strip(),
                    "name_short": row.get("name_short", "").strip(),
                    "gender": row.get("gender", "").strip(),
                    "function": row.get("function", "").strip(),
                    "country_code": row.get("country_code", "").strip(),
                    "nationality_code": row.get("nationality_code", "").strip(),
                    "height": row.get("height", "").strip(),
                    "weight": row.get("weight", "").strip(),
                    "disciplines": ";".join(parse_list(row.get("disciplines", ""))),
                    "events": ";".join(parse_list(row.get("events", ""))),
                    "birth_date": row.get("birth_date", "").strip(),
                    "birth_place": row.get("birth_place", "").strip(),
                }
            )
            country = row.get("country_code", "").strip()
            if country and country in valid_countries:
                athlete_country.append(
                    {
                        ":START_ID(Athlete-ID)": code,
                        ":END_ID(Country-ID)": country,
                        ":TYPE": "REPRESENTS",
                    }
                )

    coach_rows = []
    coach_country = []
    with coaches_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["code"].strip()
            coach_rows.append(
                {
                    "coach_code:ID(Coach-ID)": code,
                    "name": row.get("name", "").strip(),
                    "gender": row.get("gender", "").strip(),
                    "function": row.get("function", "").strip(),
                    "category": row.get("category", "").strip(),
                    "country_code": row.get("country_code", "").strip(),
                    "country": row.get("country", "").strip(),
                }
            )
            country = row.get("country_code", "").strip()
            if country and country in valid_countries:
                coach_country.append(
                    {
                        ":START_ID(Coach-ID)": code,
                        ":END_ID(Country-ID)": country,
                        ":TYPE": "REPRESENTS",
                    }
                )

    official_rows = []
    official_country = []
    with officials_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["code"].strip()
            official_rows.append(
                {
                    "official_code:ID(Official-ID)": code,
                    "name": row.get("name", "").strip(),
                    "gender": row.get("gender", "").strip(),
                    "function": row.get("function", "").strip(),
                    "category": row.get("category", "").strip(),
                    "organisation_code": row.get("organisation_code", "").strip(),
                    "organisation": row.get("organisation", "").strip(),
                    "disciplines": ";".join(parse_list(row.get("disciplines", ""))),
                }
            )
            country = row.get("organisation_code", "").strip()
            if country and country in valid_countries:
                official_country.append(
                    {
                        ":START_ID(Official-ID)": code,
                        ":END_ID(Country-ID)": country,
                        ":TYPE": "REPRESENTS",
                    }
                )

    write_csv(
        OUT_DIR / "nodes_athletes.csv",
        [
            "athlete_code:ID(Athlete-ID)",
            "name",
            "name_short",
            "gender",
            "function",
            "country_code",
            "nationality_code",
            "height",
            "weight",
            "disciplines",
            "events",
            "birth_date",
            "birth_place",
        ],
        sorted(athlete_rows, key=lambda item: item["athlete_code:ID(Athlete-ID)"]),
    )
    write_csv(
        OUT_DIR / "nodes_coaches.csv",
        [
            "coach_code:ID(Coach-ID)",
            "name",
            "gender",
            "function",
            "category",
            "country_code",
            "country",
        ],
        sorted(coach_rows, key=lambda item: item["coach_code:ID(Coach-ID)"]),
    )
    write_csv(
        OUT_DIR / "nodes_officials.csv",
        [
            "official_code:ID(Official-ID)",
            "name",
            "gender",
            "function",
            "category",
            "organisation_code",
            "organisation",
            "disciplines",
        ],
        sorted(official_rows, key=lambda item: item["official_code:ID(Official-ID)"]),
    )

    write_csv(
        OUT_DIR / "rels_athlete_country.csv",
        [":START_ID(Athlete-ID)", ":END_ID(Country-ID)", ":TYPE"],
        sorted(athlete_country, key=lambda item: (item[":START_ID(Athlete-ID)"], item[":END_ID(Country-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_coach_country.csv",
        [":START_ID(Coach-ID)", ":END_ID(Country-ID)", ":TYPE"],
        sorted(coach_country, key=lambda item: (item[":START_ID(Coach-ID)"], item[":END_ID(Country-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_official_country.csv",
        [":START_ID(Official-ID)", ":END_ID(Country-ID)", ":TYPE"],
        sorted(official_country, key=lambda item: (item[":START_ID(Official-ID)"], item[":END_ID(Country-ID)"])),
    )


def build_teams(valid_countries: dict[str, dict]):
    teams_path = IMPORT_DIR / "teams.csv"
    team_rows = []
    team_country = []
    athlete_team = []
    coach_team = []

    with teams_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row["code"].strip()
            discipline_code = row.get("disciplines_code", "").strip()
            team_rows.append(
                {
                    "team_code:ID(Team-ID)": code,
                    "name": row.get("team", "").strip(),
                    "gender": row.get("team_gender", "").strip(),
                    "country_code": row.get("country_code", "").strip(),
                    "country": row.get("country", "").strip(),
                    "discipline": row.get("discipline", "").strip(),
                    "discipline_code": discipline_code,
                    "events": row.get("events", "").strip(),
                    "num_athletes": row.get("num_athletes", "").strip(),
                    "num_coaches": row.get("num_coaches", "").strip(),
                }
            )
            country = row.get("country_code", "").strip()
            if country and country in valid_countries:
                team_country.append(
                    {
                        ":START_ID(Team-ID)": code,
                        ":END_ID(Country-ID)": country,
                        ":TYPE": "REPRESENTS",
                    }
                )

            athlete_codes = parse_list(row.get("athletes_codes", ""))
            for athlete_code in athlete_codes:
                athlete_team.append(
                    {
                        ":START_ID(Athlete-ID)": athlete_code,
                        ":END_ID(Team-ID)": code,
                        ":TYPE": "MEMBER_OF",
                    }
                )

            coach_codes = parse_list(row.get("coaches_codes", ""))
            for coach_code in coach_codes:
                coach_team.append(
                    {
                        ":START_ID(Coach-ID)": coach_code,
                        ":END_ID(Team-ID)": code,
                        ":TYPE": "COACHES",
                    }
                )

    write_csv(
        OUT_DIR / "nodes_teams.csv",
        [
            "team_code:ID(Team-ID)",
            "name",
            "gender",
            "country_code",
            "country",
            "discipline",
            "discipline_code",
            "events",
            "num_athletes",
            "num_coaches",
        ],
        sorted(team_rows, key=lambda item: item["team_code:ID(Team-ID)"]),
    )
    write_csv(
        OUT_DIR / "rels_team_country.csv",
        [":START_ID(Team-ID)", ":END_ID(Country-ID)", ":TYPE"],
        sorted(team_country, key=lambda item: (item[":START_ID(Team-ID)"], item[":END_ID(Country-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_athlete_team.csv",
        [":START_ID(Athlete-ID)", ":END_ID(Team-ID)", ":TYPE"],
        sorted(athlete_team, key=lambda item: (item[":START_ID(Athlete-ID)"], item[":END_ID(Team-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_coach_team.csv",
        [":START_ID(Coach-ID)", ":END_ID(Team-ID)", ":TYPE"],
        sorted(coach_team, key=lambda item: (item[":START_ID(Coach-ID)"], item[":END_ID(Team-ID)"])),
    )


def parse_results(venue_name_lookup):
    event_info = {}
    event_lookup = {}
    athlete_results = []
    team_results = []

    for result_file in sorted(RESULTS_DIR.glob("*.csv")):
        with result_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                event_code = row.get("event_code", "").strip()
                if not event_code:
                    continue
                discipline_name = row.get("discipline_name", "").strip()
                discipline_code = row.get("discipline_code", "").strip()
                event_name = row.get("event_name", "").strip()
                event_stage = row.get("event_stage", "").strip()
                gender = row.get("gender", "").strip()
                venue_name = row.get("venue", "").strip()
                venue_code = venue_name_lookup.get(venue_name, "")
                info = event_info.setdefault(
                    event_code,
                    {
                        "event_code:ID(Event-ID)": event_code,
                        "name": event_name,
                        "sport_name": discipline_name,
                        "sport_code": discipline_code,
                        "gender": gender,
                        "stages": set(),
                        "venues": set(),
                        "has_medal": False,
                    },
                )
                if event_name and not info["name"]:
                    info["name"] = event_name
                if gender and not info["gender"]:
                    info["gender"] = gender
                if discipline_name and not info["sport_name"]:
                    info["sport_name"] = discipline_name
                if discipline_code and not info["sport_code"]:
                    info["sport_code"] = discipline_code
                if event_stage:
                    info["stages"].add(event_stage)
                if venue_code:
                    info["venues"].add(venue_code)
                event_lookup[(discipline_name.lower(), event_name.lower())] = event_code

                participant_code = row.get("participant_code", "").strip()
                if not participant_code:
                    continue
                participant_type = row.get("participant_type", "").strip().lower()
                base_payload = {
                    "event_code": event_code,
                    "stage_code": row.get("stage_code", "").strip(),
                    "event_stage": event_stage,
                    "stage": row.get("stage", "").strip(),
                    "date": row.get("date", "").strip(),
                    "result": row.get("result", "").strip(),
                    "result_type": row.get("result_type", "").strip(),
                    "result_status": row.get("result_IRM", "").strip() or row.get("result_WLT", "").strip(),
                    "result_diff": row.get("result_diff", "").strip(),
                    "bib": row.get("bib", "").strip() or row.get("start_order", "").strip(),
                    "rank": row.get("rank", "").strip(),
                    "country_code": row.get("participant_country_code", "").strip(),
                }
                if participant_type == "person":
                    base_payload["athlete_code"] = participant_code
                    athlete_results.append(base_payload)
                elif participant_type == "team":
                    base_payload["team_code"] = participant_code
                    team_results.append(base_payload)

    return event_info, event_lookup, athlete_results, team_results


def build_event_nodes(event_info):
    records = []
    event_sport = []
    event_venue = []
    for event_code, info in sorted(event_info.items()):
        records.append(
            {
                "event_code:ID(Event-ID)": event_code,
                "name": info["name"],
                "gender": info["gender"],
                "sport_name": info["sport_name"],
                "sport_code": info["sport_code"],
                "stages": ";".join(sorted(info["stages"])),
                "has_medalists": "True" if info["has_medal"] else "False",
            }
        )
        if info["sport_code"]:
            event_sport.append(
                {
                    ":START_ID(Event-ID)": event_code,
                    ":END_ID(Sport-ID)": info["sport_code"],
                    ":TYPE": "PART_OF",
                }
            )
        for venue_code in sorted(info["venues"]):
            event_venue.append(
                {
                    ":START_ID(Event-ID)": event_code,
                    ":END_ID(Venue-ID)": venue_code,
                    ":TYPE": "HOSTED_AT",
                }
            )
    write_csv(
        OUT_DIR / "nodes_events.csv",
        [
            "event_code:ID(Event-ID)",
            "name",
            "gender",
            "sport_name",
            "sport_code",
            "stages",
            "has_medalists",
        ],
        records,
    )
    write_csv(
        OUT_DIR / "rels_event_sport.csv",
        [":START_ID(Event-ID)", ":END_ID(Sport-ID)", ":TYPE"],
        sorted(event_sport, key=lambda item: (item[":START_ID(Event-ID)"], item[":END_ID(Sport-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_event_venue.csv",
        [":START_ID(Event-ID)", ":END_ID(Venue-ID)", ":TYPE"],
        sorted(event_venue, key=lambda item: (item[":START_ID(Event-ID)"], item[":END_ID(Venue-ID)"])),
    )


def build_participation_files(athlete_results, team_results):
    athlete_rows = [
        {
            ":START_ID(Athlete-ID)": row["athlete_code"],
            ":END_ID(Event-ID)": row["event_code"],
            "stage_code": row["stage_code"],
            "event_stage": row["event_stage"],
            "stage": row["stage"],
            "date": row["date"],
            "result": row["result"],
            "result_type": row["result_type"],
            "result_status": row["result_status"],
            "result_diff": row["result_diff"],
            "bib": row["bib"],
            "rank": row["rank"],
            ":TYPE": "COMPETED_IN",
        }
        for row in athlete_results
        if row.get("athlete_code")
    ]
    write_csv(
        OUT_DIR / "rels_athlete_event_results.csv",
        [
            ":START_ID(Athlete-ID)",
            ":END_ID(Event-ID)",
            "stage_code",
            "event_stage",
            "stage",
            "date",
            "result",
            "result_type",
            "result_status",
            "result_diff",
            "bib",
            "rank",
            ":TYPE",
        ],
        sorted(athlete_rows, key=lambda item: (item[":START_ID(Athlete-ID)"], item[":END_ID(Event-ID)"], item["stage_code"])),
    )

    team_rows = [
        {
            ":START_ID(Team-ID)": row["team_code"],
            ":END_ID(Event-ID)": row["event_code"],
            "stage_code": row["stage_code"],
            "event_stage": row["event_stage"],
            "stage": row["stage"],
            "date": row["date"],
            "result": row["result"],
            "result_type": row["result_type"],
            "result_status": row["result_status"],
            "result_diff": row["result_diff"],
            "bib": row["bib"],
            "rank": row["rank"],
            ":TYPE": "COMPETED_IN",
        }
        for row in team_results
        if row.get("team_code")
    ]
    write_csv(
        OUT_DIR / "rels_team_event_results.csv",
        [
            ":START_ID(Team-ID)",
            ":END_ID(Event-ID)",
            "stage_code",
            "event_stage",
            "stage",
            "date",
            "result",
            "result_type",
            "result_status",
            "result_diff",
            "bib",
            "rank",
            ":TYPE",
        ],
        sorted(team_rows, key=lambda item: (item[":START_ID(Team-ID)"], item[":END_ID(Event-ID)"], item["stage_code"])),
    )


def build_medal_relationships(event_info, event_lookup):
    medallist_path = IMPORT_DIR / "medallists.csv"
    athlete_medals = []
    team_medals = []

    with medallist_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            discipline = row.get("discipline", "").strip().lower()
            event = row.get("event", "").strip().lower()
            key = (discipline, event)
            event_code = event_lookup.get(key)
            if not event_code:
                discipline_name = row.get("discipline", "").strip()
                fallback_code = row.get("nationality_code", "").strip() or "GEN"
                event_code = f"{fallback_code}_{slugify(row.get('event', ''))}".upper()
                if event_code not in event_info:
                    event_info[event_code] = {
                        "event_code:ID(Event-ID)": event_code,
                        "name": row.get("event", "").strip(),
                        "sport_name": discipline_name,
                        "sport_code": "",
                        "gender": row.get("gender", "").strip(),
                        "stages": set(),
                        "venues": set(),
                        "has_medal": True,
                    }
            event_info[event_code]["has_medal"] = True
            payload = {
                "event_code": event_code,
                "medal_type": row.get("medal_type", "").strip(),
                "medal_date": row.get("medal_date", "").strip(),
            }
            athlete_code = row.get("code_athlete", "").strip()
            team_code = row.get("code_team", "").strip()
            if athlete_code:
                athlete_medals.append(
                    {
                        ":START_ID(Athlete-ID)": athlete_code,
                        ":END_ID(Event-ID)": event_code,
                        "medal_type": payload["medal_type"],
                        "medal_date": payload["medal_date"],
                        ":TYPE": "WON_MEDAL",
                    }
                )
            elif team_code:
                team_medals.append(
                    {
                        ":START_ID(Team-ID)": team_code,
                        ":END_ID(Event-ID)": event_code,
                        "medal_type": payload["medal_type"],
                        "medal_date": payload["medal_date"],
                        ":TYPE": "WON_MEDAL",
                    }
                )

    write_csv(
        OUT_DIR / "rels_athlete_medals.csv",
        [":START_ID(Athlete-ID)", ":END_ID(Event-ID)", "medal_type", "medal_date", ":TYPE"],
        sorted(athlete_medals, key=lambda item: (item[":START_ID(Athlete-ID)"], item[":END_ID(Event-ID)"])),
    )
    write_csv(
        OUT_DIR / "rels_team_medals.csv",
        [":START_ID(Team-ID)", ":END_ID(Event-ID)", "medal_type", "medal_date", ":TYPE"],
        sorted(team_medals, key=lambda item: (item[":START_ID(Team-ID)"], item[":END_ID(Event-ID)"])),
    )


def main():
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    countries = build_countries()
    build_sports()
    venue_lookup = build_venues_and_sessions()
    build_people(countries)
    build_teams(countries)
    event_info, event_lookup, athlete_results, team_results = parse_results(venue_lookup)
    build_medal_relationships(event_info, event_lookup)
    build_event_nodes(event_info)
    build_participation_files(athlete_results, team_results)


if __name__ == "__main__":
    main()
