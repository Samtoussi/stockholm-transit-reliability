import io
import os
import zipfile

import pandas as pd
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

REALTIME_API_KEY = os.getenv("TRAFIKLAB_REALTIME_API_KEY")
STATIC_API_KEY = os.getenv("TRAFIKLAB_STATIC_API_KEY")

if not REALTIME_API_KEY:
    raise ValueError("TRAFIKLAB_REALTIME_API_KEY is missing from .env")

if not STATIC_API_KEY:
    raise ValueError("TRAFIKLAB_STATIC_API_KEY is missing from .env")


VEHICLE_POSITIONS_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/VehiclePositionsSweden.pb"
)

STATIC_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-sweden/sweden.zip"
)


# --------------------------------------------------
# Fetch live VehiclePositions
# --------------------------------------------------

def fetch_live_vehicles():
    response = requests.get(
        VEHICLE_POSITIONS_URL,
        params={"key": REALTIME_API_KEY},
        timeout=30,
    )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    vehicles = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        vehicle_id = vehicle.vehicle.id
        trip_id = vehicle.trip.trip_id

        if not vehicle_id or not trip_id:
            continue

        vehicles.append(
            {
                "vehicle_id": vehicle_id,
                "trip_id": trip_id,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "timestamp": vehicle.timestamp,
            }
        )

    return vehicles


# --------------------------------------------------
# Download static GTFS
# --------------------------------------------------

def fetch_static_gtfs():
    print("Downloading GTFS Sweden static...")

    response = requests.get(
        STATIC_URL,
        params={"key": STATIC_API_KEY},
        timeout=120,
    )
    response.raise_for_status()

    return response.content


# --------------------------------------------------
# Load trips + routes
# --------------------------------------------------

def load_gtfs_lookup(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as gtfs_zip:
        trips = pd.read_csv(
            gtfs_zip.open("trips.txt"),
            dtype=str,
            usecols=[
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
            ],
        )

        routes = pd.read_csv(
            gtfs_zip.open("routes.txt"),
            dtype=str,
            usecols=[
                "route_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ],
        )

    lookup = trips.merge(
        routes,
        on="route_id",
        how="left",
    )

    return lookup


# --------------------------------------------------
# Match live trips to static GTFS
# --------------------------------------------------

def main():
    print("=" * 70)
    print("SL LIVE VEHICLE → STATIC ROUTE CONTEXT TEST")
    print("=" * 70)

    print("\nFetching live VehiclePositions...")
    vehicles = fetch_live_vehicles()

    print(f"Live vehicles with trip_id: {len(vehicles)}")

    static_zip = fetch_static_gtfs()

    print(f"Static GTFS downloaded: {len(static_zip) / 1024 / 1024:.2f} MB")

    print("\nLoading trips.txt + routes.txt...")
    lookup = load_gtfs_lookup(static_zip)

    print(f"Static trips loaded: {len(lookup)}")

    lookup = lookup.set_index("trip_id")

    matched = []
    unmatched = []

    for vehicle in vehicles:
        trip_id = vehicle["trip_id"]

        if trip_id not in lookup.index:
            unmatched.append(vehicle)
            continue

        route = lookup.loc[trip_id]

        # Defensive handling in case trip_id somehow appears more than once
        if isinstance(route, pd.DataFrame):
            route = route.iloc[0]

        matched.append(
            {
                **vehicle,
                "route_id": route["route_id"],
                "route_short_name": route["route_short_name"],
                "route_long_name": route["route_long_name"],
                "route_type": route["route_type"],
                "trip_headsign": route["trip_headsign"],
            }
        )

    print("\n" + "=" * 70)
    print("RESULT")
    print("=" * 70)

    print(f"Live vehicles:           {len(vehicles)}")
    print(f"Matched to static GTFS:  {len(matched)}")
    print(f"Unmatched:               {len(unmatched)}")

    if vehicles:
        match_pct = len(matched) / len(vehicles) * 100
        print(f"Match rate:              {match_pct:.1f}%")

    print("\nExample matched vehicles:")

    for vehicle in matched[:10]:
        print("-" * 70)
        print(f"Vehicle:       {vehicle['vehicle_id']}")
        print(f"Trip:          {vehicle['trip_id']}")
        print(f"Route ID:      {vehicle['route_id']}")
        print(f"Route:         {vehicle['route_short_name']}")
        print(f"Route name:    {vehicle['route_long_name']}")
        print(f"Headsign:      {vehicle['trip_headsign']}")
        print(f"Route type:    {vehicle['route_type']}")
        print(
            f"Position:      "
            f"{vehicle['latitude']}, {vehicle['longitude']}"
        )

    if unmatched:
        print("\nExample unmatched trip IDs:")

        for vehicle in unmatched[:10]:
            print(
                f"{vehicle['trip_id']} "
                f"(vehicle {vehicle['vehicle_id']})"
            )


if __name__ == "__main__":
    main()