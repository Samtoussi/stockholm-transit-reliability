import csv
from pathlib import Path


DATA_DIR = Path("data/raw/gtfs_static")


# 1. Hitta route 13 / Röda linjen
with open(DATA_DIR / "routes.txt", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    route_13 = None

    for row in reader:
        if row["route_short_name"] == "13" and row["route_long_name"] == "Röda linjen":
            route_13 = row
            break


if route_13 is None:
    raise ValueError("Route 13 / Röda linjen not found")


route_id = route_13["route_id"]

print("ROUTE")
print(f"route_id: {route_id}")
print(f"name: {route_13['route_short_name']} - {route_13['route_long_name']}")
print(f"type: {route_13['route_type']}")


# 2. Hitta några trips som tillhör samma route
print("\nTRIPS")

trip_count = 0
trip_ids = []

with open(DATA_DIR / "trips.txt", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["route_id"] == route_id:
            print(
                f"trip_id={row['trip_id']} | "
                f"service_id={row['service_id']} | "
                f"headsign={row['trip_headsign']} | "
                f"direction_id={row['direction_id']}"
            )

            trip_ids.append(row["trip_id"])
            trip_count += 1

            if trip_count == 10:
                break


if not trip_ids:
    raise ValueError("No trips found for route 13")


# 3. Välj första trippen
selected_trip_id = trip_ids[0]

print(f"\nSELECTED TRIP")
print(f"trip_id: {selected_trip_id}")


# 4. Hämta alla stop_times för vald trip
stop_times = []

with open(DATA_DIR / "stop_times.txt", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if row["trip_id"] == selected_trip_id:
            stop_times.append(row)


if not stop_times:
    raise ValueError(f"No stop times found for trip {selected_trip_id}")


# 5. Bygg lookup från stop_id -> stop_name
stops = {}

with open(DATA_DIR / "stops.txt", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        stops[row["stop_id"]] = row["stop_name"]


# 6. Visa mänskligt läsbar timetable
print(f"\nSCHEDULE FOR TRIP {selected_trip_id}")

for stop_time in stop_times:
    stop_name = stops.get(stop_time["stop_id"], "Unknown stop")

    print(
        f"{stop_time['stop_sequence']:>2}. "
        f"{stop_name:<30} | "
        f"arrival={stop_time['arrival_time']} | "
        f"departure={stop_time['departure_time']}"
    )