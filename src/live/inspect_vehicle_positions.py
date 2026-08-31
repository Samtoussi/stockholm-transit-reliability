import os
import time

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("TRAFIKLAB_REALTIME_API_KEY")

if not API_KEY:
    raise ValueError("TRAFIKLAB_REALTIME_API_KEY is missing from .env")


VEHICLE_POSITIONS_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/VehiclePositionsSweden.pb"
)


# --------------------------------------------------
# Fetch
# --------------------------------------------------

def fetch_positions():
    response = requests.get(
        VEHICLE_POSITIONS_URL,
        params={"key": API_KEY},
        timeout=30,
    )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    vehicles = {}

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        if not vehicle.vehicle.id:
            continue

        vehicles[vehicle.vehicle.id] = {
            "trip_id": vehicle.trip.trip_id,
            "route_id": vehicle.trip.route_id,
            "latitude": vehicle.position.latitude,
            "longitude": vehicle.position.longitude,
            "timestamp": vehicle.timestamp,
        }

    return vehicles, feed.header.timestamp


# --------------------------------------------------
# Compare snapshots
# --------------------------------------------------

def main():
    print("=" * 60)
    print("SL VEHICLE POSITIONS — UPDATE TEST")
    print("=" * 60)

    # Snapshot 1
    print("\nFetching snapshot #1...")

    first, first_feed_timestamp = fetch_positions()

    print(f"Vehicles found: {len(first)}")
    print(f"Feed timestamp: {first_feed_timestamp}")

    # Wait
    wait_seconds = 10

    print(f"\nWaiting {wait_seconds} seconds...")
    time.sleep(wait_seconds)

    # Snapshot 2
    print("\nFetching snapshot #2...")

    second, second_feed_timestamp = fetch_positions()

    print(f"Vehicles found: {len(second)}")
    print(f"Feed timestamp: {second_feed_timestamp}")

    # Vehicles existing in both snapshots
    common_ids = set(first) & set(second)

    changed = []

    position_changes = 0
    timestamp_changes = 0

    for vehicle_id in common_ids:
        before = first[vehicle_id]
        after = second[vehicle_id]

        position_changed = (
            before["latitude"] != after["latitude"]
            or before["longitude"] != after["longitude"]
        )

        timestamp_changed = (
            before["timestamp"] != after["timestamp"]
        )

        if position_changed:
            position_changes += 1

        if timestamp_changed:
            timestamp_changes += 1

        if position_changed or timestamp_changed:
            changed.append(
                (
                    vehicle_id,
                    before,
                    after,
                    position_changed,
                    timestamp_changed,
                )
            )

    # --------------------------------------------------
    # Result
    # --------------------------------------------------

    print("\n" + "=" * 60)
    print("RESULT")
    print("=" * 60)

    print(f"Vehicles in snapshot #1:       {len(first)}")
    print(f"Vehicles in snapshot #2:       {len(second)}")
    print(f"Vehicles present in both:      {len(common_ids)}")
    print(f"Vehicles with updated data:    {len(changed)}")
    print(f"Vehicles with new position:    {position_changes}")
    print(f"Vehicles with new timestamp:   {timestamp_changes}")

    if common_ids:
        update_pct = len(changed) / len(common_ids) * 100
        position_pct = position_changes / len(common_ids) * 100

        print(f"Updated vehicles:              {update_pct:.1f}%")
        print(f"Position updates:              {position_pct:.1f}%")

    print()
    print(f"Feed timestamp #1:             {first_feed_timestamp}")
    print(f"Feed timestamp #2:             {second_feed_timestamp}")
    print(
        f"Feed snapshot changed:         "
        f"{first_feed_timestamp != second_feed_timestamp}"
    )

    if first_feed_timestamp and second_feed_timestamp:
        feed_delta = second_feed_timestamp - first_feed_timestamp

        print(
            f"Feed timestamp difference:      "
            f"{feed_delta} seconds"
        )

    # --------------------------------------------------
    # Example updates
    # --------------------------------------------------

    print("\nExample updates:")

    if not changed:
        print("No vehicle updates detected.")

    for (
        vehicle_id,
        before,
        after,
        position_changed,
        timestamp_changed,
    ) in changed[:10]:

        print("-" * 60)
        print(f"Vehicle:           {vehicle_id}")
        print(f"Trip:              {after['trip_id']}")
        print(f"Route:             {after['route_id']}")
        print(f"Position changed:  {position_changed}")
        print(f"Timestamp changed: {timestamp_changed}")

        print(
            f"Before:            "
            f"{before['latitude']}, "
            f"{before['longitude']} "
            f"(timestamp {before['timestamp']})"
        )

        print(
            f"After:             "
            f"{after['latitude']}, "
            f"{after['longitude']} "
            f"(timestamp {after['timestamp']})"
        )


if __name__ == "__main__":
    main()