import csv
from pathlib import Path
from google.transit import gtfs_realtime_pb2


DATA_FILE = Path("data/raw/gtfs_realtime/TripUpdatesSweden.pb")


feed = gtfs_realtime_pb2.FeedMessage()

with open(DATA_FILE, "rb") as file:
    feed.ParseFromString(file.read())


print("FEED HEADER")
print(f"GTFS-RT version: {feed.header.gtfs_realtime_version}")
print(f"timestamp: {feed.header.timestamp}")
print(f"entities: {len(feed.entity)}")


print("\nFIRST TRIP UPDATE")

for entity in feed.entity:
    if entity.HasField("trip_update"):
        trip_update = entity.trip_update

        print(f"entity_id: {entity.id}")
        print(f"trip_id: {trip_update.trip.trip_id}")
        print(f"route_id: {trip_update.trip.route_id}")
        print(f"start_date: {trip_update.trip.start_date}")
        print(f"start_time: {trip_update.trip.start_time}")
        print(f"stop updates: {len(trip_update.stop_time_update)}")

        print("\nSTOP TIME UPDATES")

        for stop_update in trip_update.stop_time_update[:10]:
            print(
                f"stop_id={stop_update.stop_id} | "
                f"arrival_time={stop_update.arrival.time} | "
                f"arrival_delay={stop_update.arrival.delay} | "
                f"departure_time={stop_update.departure.time} | "
                f"departure_delay={stop_update.departure.delay}"
            )

        break

    import csv


STATIC_TRIPS_FILE = Path("data/raw/gtfs_static/trips.txt")


realtime_trip_id = trip_update.trip.trip_id

print("\nSTATIC ↔ REALTIME HANDSHAKE")
print(f"Looking for trip_id: {realtime_trip_id}")


matched_trip = None

with open(STATIC_TRIPS_FILE, encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["trip_id"] == realtime_trip_id:
            matched_trip = row
            break


if matched_trip is None:
    print("❌ No matching trip found in static trips.txt")
else:
    print("✅ MATCH FOUND")
    print(f"route_id: {matched_trip['route_id']}")
    print(f"service_id: {matched_trip['service_id']}")
    print(f"direction_id: {matched_trip['direction_id']}")
    print(f"shape_id: {matched_trip['shape_id']}")

    ROUTES_FILE = Path("data/raw/gtfs_static/routes.txt")
STOPS_FILE = Path("data/raw/gtfs_static/stops.txt")


# 1. Hämta route-information från static
matched_route = None

with open(ROUTES_FILE, encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["route_id"] == matched_trip["route_id"]:
            matched_route = row
            break


print("\nROUTE DETAILS")

if matched_route is None:
    print("❌ Route not found")
else:
    print(f"route_short_name: {matched_route['route_short_name']}")
    print(f"route_long_name: {matched_route['route_long_name']}")
    print(f"route_type: {matched_route['route_type']}")


# 2. Bygg lookup för stop_id -> stop_name
stops = {}

with open(STOPS_FILE, encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        stops[row["stop_id"]] = row["stop_name"]


# 3. Visa realtime-stoppar med mänskliga namn
print("\nREALTIME STOP UPDATES")

for stop_update in trip_update.stop_time_update:
    stop_name = stops.get(stop_update.stop_id, "Unknown stop")

    print(
        f"{stop_name:<30} | "
        f"arrival_delay={stop_update.arrival.delay}s | "
        f"departure_delay={stop_update.departure.delay}s"
    )

    # --------------------------------------------------
# FULL SNAPSHOT PROFILE
# --------------------------------------------------

print("\nFULL SNAPSHOT PROFILE")
print("-" * 60)

trip_updates = [
    entity.trip_update
    for entity in feed.entity
    if entity.HasField("trip_update")
]

unique_trip_ids = {
    trip_update.trip.trip_id
    for trip_update in trip_updates
    if trip_update.trip.trip_id
}

total_stop_updates = sum(
    len(trip_update.stop_time_update)
    for trip_update in trip_updates
)

print(f"TripUpdates: {len(trip_updates):,}")
print(f"Unique trip_ids: {len(unique_trip_ids):,}")
print(f"StopTimeUpdates: {total_stop_updates:,}")


# --------------------------------------------------
# STATIC TRIP MATCH RATE
# --------------------------------------------------

print("\nSTATIC TRIP MATCH RATE")
print("-" * 60)

static_trip_ids = set()

with open(
    STATIC_TRIPS_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        static_trip_ids.add(row["trip_id"])

matched_trip_ids = unique_trip_ids & static_trip_ids
unmatched_trip_ids = unique_trip_ids - static_trip_ids

print(f"Realtime trip_ids: {len(unique_trip_ids):,}")
print(f"Matched static: {len(matched_trip_ids):,}")
print(f"Unmatched static: {len(unmatched_trip_ids):,}")

if unique_trip_ids:
    match_rate = (
        len(matched_trip_ids)
        / len(unique_trip_ids)
        * 100
    )

    print(f"Match rate: {match_rate:.2f}%")


# --------------------------------------------------
# DELAY PROFILE
# --------------------------------------------------

print("\nDELAY PROFILE")
print("-" * 60)

arrival_delays = []
departure_delays = []

for trip_update in trip_updates:
    for stop_update in trip_update.stop_time_update:

        if stop_update.HasField("arrival"):
            if stop_update.arrival.HasField("delay"):
                arrival_delays.append(
                    stop_update.arrival.delay
                )

        if stop_update.HasField("departure"):
            if stop_update.departure.HasField("delay"):
                departure_delays.append(
                    stop_update.departure.delay
                )


def print_delay_profile(name, values):
    if not values:
        print(f"{name}: no delay values")
        return

    sorted_values = sorted(values)

    avg_delay = sum(values) / len(values)

    print(f"{name}")
    print(f"  observations: {len(values):,}")
    print(f"  min: {min(values):,} s")
    print(f"  max: {max(values):,} s")
    print(f"  avg: {avg_delay:.2f} s")


print_delay_profile(
    "Arrival delays",
    arrival_delays,
)

print_delay_profile(
    "Departure delays",
    departure_delays,
)


# --------------------------------------------------
# ROUTE PROFILE
# --------------------------------------------------

print("\nROUTE PROFILE")
print("-" * 60)

route_counts = {}

for trip_update in trip_updates:
    route_id = trip_update.trip.route_id

    if not route_id:
        continue

    route_counts[route_id] = (
        route_counts.get(route_id, 0) + 1
    )

top_routes = sorted(
    route_counts.items(),
    key=lambda item: item[1],
    reverse=True,
)[:20]

for route_id, count in top_routes:
    print(
        f"{route_id:<25} "
        f"{count:>8,} trip updates"
    )

# --------------------------------------------------
# STATIC ROUTE LOOKUP
# --------------------------------------------------

print("\nSTATIC ROUTE PROFILE")
print("-" * 60)

# trip_id -> route_id
trip_to_route = {}

with open(
    STATIC_TRIPS_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        trip_to_route[row["trip_id"]] = row["route_id"]


# route_id -> route details
route_lookup = {}

with open(
    ROUTES_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        route_lookup[row["route_id"]] = {
            "route_short_name": row["route_short_name"],
            "route_long_name": row["route_long_name"],
            "route_type": row["route_type"],
        }


# Count realtime trips per static route
static_route_counts = {}

for trip_update in trip_updates:
    trip_id = trip_update.trip.trip_id

    route_id = trip_to_route.get(trip_id)

    if not route_id:
        continue

    static_route_counts[route_id] = (
        static_route_counts.get(route_id, 0) + 1
    )


top_static_routes = sorted(
    static_route_counts.items(),
    key=lambda item: item[1],
    reverse=True,
)[:20]


for route_id, count in top_static_routes:
    route = route_lookup.get(route_id, {})

    print(
        f"{route.get('route_short_name', 'UNKNOWN'):<8} | "
        f"type={route.get('route_type', 'UNKNOWN'):<5} | "
        f"{count:>4} trip updates"
    )

# --------------------------------------------------
# ROUTE TYPE PROFILE
# --------------------------------------------------

print("\nROUTE TYPE PROFILE")
print("-" * 60)

route_type_counts = {}

for route_id, count in static_route_counts.items():
    route = route_lookup.get(route_id)

    if not route:
        continue

    route_type = route["route_type"]

    route_type_counts[route_type] = (
        route_type_counts.get(route_type, 0) + count
    )

for route_type, count in sorted(
    route_type_counts.items(),
    key=lambda item: item[1],
    reverse=True,
):
    print(
        f"route_type={route_type:<5} "
        f"{count:>5} trip updates"
    )

# --------------------------------------------------
# REALTIME ↔ SL SCOPE PROFILE
# --------------------------------------------------

print("\nREALTIME ↔ SL SCOPE PROFILE")
print("-" * 60)

SL_AGENCY_ID = "505000000000000001"

# route_id -> agency_id
route_to_agency = {}

with open(
    ROUTES_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        route_to_agency[row["route_id"]] = row["agency_id"]


sl_realtime_trip_ids = set()
outside_sl_trip_ids = set()

for trip_id in unique_trip_ids:

    route_id = trip_to_route.get(trip_id)

    if not route_id:
        continue

    agency_id = route_to_agency.get(route_id)

    if agency_id == SL_AGENCY_ID:
        sl_realtime_trip_ids.add(trip_id)
    else:
        outside_sl_trip_ids.add(trip_id)


print(f"Realtime trip_ids: {len(unique_trip_ids):,}")
print(f"Inside SL scope: {len(sl_realtime_trip_ids):,}")
print(f"Outside SL scope: {len(outside_sl_trip_ids):,}")

if unique_trip_ids:
    sl_scope_rate = (
        len(sl_realtime_trip_ids)
        / len(unique_trip_ids)
        * 100
    )

    print(f"SL scope rate: {sl_scope_rate:.2f}%")

print("\nOUTSIDE SL SCOPE EXAMPLES")
print("-" * 60)

for trip_id in sorted(outside_sl_trip_ids)[:20]:

    route_id = trip_to_route.get(trip_id)
    route = route_lookup.get(route_id, {})

    print(
        f"trip_id={trip_id} | "
        f"route={route.get('route_short_name', 'UNKNOWN')} | "
        f"type={route.get('route_type', 'UNKNOWN')} | "
        f"agency={route_to_agency.get(route_id, 'UNKNOWN')}"
    )

# --------------------------------------------------
# AGENCY 114 PROFILE
# --------------------------------------------------

print("\nAGENCY 114 PROFILE")
print("-" * 60)

TARGET_AGENCY_ID = "500000000000000114"

AGENCY_FILE = Path("data/raw/gtfs_static/agency.txt")

# Agency details
with open(
    AGENCY_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["agency_id"] == TARGET_AGENCY_ID:
            print(f"agency_id: {row['agency_id']}")
            print(f"agency_name: {row['agency_name']}")
            print(f"agency_url: {row['agency_url']}")
            break


# All routes belonging to agency 114
agency_114_routes = []

with open(
    ROUTES_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["agency_id"] == TARGET_AGENCY_ID:
            agency_114_routes.append(row)


print(f"\nRoutes: {len(agency_114_routes):,}")

print("\nAGENCY 114 ROUTES")
print("-" * 60)

for route in sorted(
    agency_114_routes,
    key=lambda row: row["route_short_name"],
):
    print(
        f"{route['route_short_name']:<8} | "
        f"type={route['route_type']:<5} | "
        f"{route['route_long_name']}"
    )

# --------------------------------------------------
# AGENCY 114 STOP PROFILE
# --------------------------------------------------

print("\nAGENCY 114 STOP PROFILE")
print("-" * 60)

agency_114_route_ids = {
    route["route_id"]
    for route in agency_114_routes
}

agency_114_trip_ids = set()

with open(
    STATIC_TRIPS_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["route_id"] in agency_114_route_ids:
            agency_114_trip_ids.add(row["trip_id"])


STOP_TIMES_FILE = Path(
    "data/raw/gtfs_static/stop_times.txt"
)

agency_114_stop_ids = set()

with open(
    STOP_TIMES_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["trip_id"] in agency_114_trip_ids:
            agency_114_stop_ids.add(row["stop_id"])


agency_114_stop_names = []

with open(
    STOPS_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["stop_id"] in agency_114_stop_ids:
            agency_114_stop_names.append(row["stop_name"])


print(f"Trips: {len(agency_114_trip_ids):,}")
print(f"Stops: {len(agency_114_stop_ids):,}")

print("\nSTOP EXAMPLES")
print("-" * 60)

for stop_name in sorted(set(agency_114_stop_names))[:50]:
    print(stop_name)

# --------------------------------------------------
# UNMATCHED TRIP EXAMPLES
# --------------------------------------------------

# --------------------------------------------------
# AGENCY 607 PROFILE
# --------------------------------------------------

print("\nAGENCY 607 PROFILE")
print("-" * 60)

TARGET_AGENCY_ID = "505000000000000607"

# Agency details
with open(
    AGENCY_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["agency_id"] == TARGET_AGENCY_ID:
            print(f"agency_id: {row['agency_id']}")
            print(f"agency_name: {row['agency_name']}")
            print(f"agency_url: {row['agency_url']}")
            break


# Routes belonging to agency 607
agency_607_routes = []

with open(
    ROUTES_FILE,
    encoding="utf-8-sig",
    newline="",
) as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["agency_id"] == TARGET_AGENCY_ID:
            agency_607_routes.append(row)


print(f"\nRoutes: {len(agency_607_routes):,}")

print("\nAGENCY 607 ROUTES")
print("-" * 60)

for route in sorted(
    agency_607_routes,
    key=lambda row: row["route_short_name"],
):
    print(
        f"{route['route_short_name']:<8} | "
        f"type={route['route_type']:<5} | "
        f"{route['route_long_name']}"
    )

print("\nUNMATCHED TRIP EXAMPLES")
print("-" * 60)

for trip_id in sorted(unmatched_trip_ids)[:20]:
    print(trip_id)

    # --------------------------------------------------
# DUPLICATE TRIP UPDATE PROFILE
# --------------------------------------------------

print("\nDUPLICATE TRIP UPDATE PROFILE")
print("-" * 60)

trip_update_entities = {}

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue

    trip_id = entity.trip_update.trip.trip_id

    trip_update_entities.setdefault(
        trip_id,
        [],
    ).append(entity)


duplicate_trip_updates = {
    trip_id: entities
    for trip_id, entities in trip_update_entities.items()
    if len(entities) > 1
}


print(
    f"Trip IDs with multiple TripUpdates: "
    f"{len(duplicate_trip_updates):,}"
)


for trip_id, entities in duplicate_trip_updates.items():

    print(f"\ntrip_id: {trip_id}")
    print(f"entities: {len(entities)}")

    for entity in entities:

        trip_update = entity.trip_update

        stop_sequences = [
            stop_update.stop_sequence
            for stop_update
            in trip_update.stop_time_update
        ]

        print(
            f"  entity_id={entity.id} | "
            f"stop_updates={len(stop_sequences)} | "
            f"sequences={stop_sequences}"
        )

        # --------------------------------------------------
# ORPHAN REALTIME STOP PROFILE
# --------------------------------------------------

print("\nORPHAN REALTIME STOP PROFILE")
print("-" * 60)

ORPHAN_STOP_IDS = {
    "9022050012197001",
    "9022050012198001",
}

for orphan_stop_id in ORPHAN_STOP_IDS:

    print(f"\nstop_id: {orphan_stop_id}")

    found_in_raw_static = False

    with open(
        STOPS_FILE,
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            if row["stop_id"] == orphan_stop_id:
                found_in_raw_static = True

                print("FOUND IN RAW STATIC")
                print(f"stop_name: {row['stop_name']}")
                print(f"location_type: {row['location_type']}")
                print(f"parent_station: {row['parent_station']}")
                print(f"platform_code: {row['platform_code']}")

                break

    if not found_in_raw_static:
        print("NOT FOUND IN RAW STATIC")