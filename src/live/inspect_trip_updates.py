import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("TRAFIKLAB_REALTIME_API_KEY")

if not API_KEY:
    raise ValueError(
        "TRAFIKLAB_REALTIME_API_KEY is missing from .env"
    )


VEHICLE_POSITIONS_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/VehiclePositionsSweden.pb"
)

TRIP_UPDATES_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/TripUpdatesSweden.pb"
)


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def format_timestamp(timestamp):
    if not timestamp:
        return None

    return datetime.fromtimestamp(
        timestamp
    ).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------
# VehiclePositions
# --------------------------------------------------

def fetch_vehicle_positions():
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

        vehicle_id = vehicle.vehicle.id
        trip_id = vehicle.trip.trip_id

        if not vehicle_id or not trip_id:
            continue

        vehicles[trip_id] = {
            "vehicle_id": vehicle_id,
            "trip_id": trip_id,
            "latitude": (
                vehicle.position.latitude
                if vehicle.HasField("position")
                else None
            ),
            "longitude": (
                vehicle.position.longitude
                if vehicle.HasField("position")
                else None
            ),
            "timestamp": vehicle.timestamp,
        }

    return vehicles, feed.header.timestamp


# --------------------------------------------------
# TripUpdates
# --------------------------------------------------

def fetch_trip_updates():
    response = requests.get(
        TRIP_UPDATES_URL,
        params={"key": API_KEY},
        timeout=30,
    )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    updates = {}

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id

        if not trip_id:
            continue

        stop_updates = []

        for stop_update in trip_update.stop_time_update:

            arrival_time = None
            arrival_delay = None

            departure_time = None
            departure_delay = None

            if stop_update.HasField("arrival"):
                if stop_update.arrival.time:
                    arrival_time = stop_update.arrival.time

                if stop_update.arrival.HasField("delay"):
                    arrival_delay = stop_update.arrival.delay

            if stop_update.HasField("departure"):
                if stop_update.departure.time:
                    departure_time = stop_update.departure.time

                if stop_update.departure.HasField("delay"):
                    departure_delay = stop_update.departure.delay

            stop_updates.append(
                {
                    "stop_sequence": stop_update.stop_sequence,
                    "stop_id": stop_update.stop_id,
                    "arrival_time": arrival_time,
                    "arrival_delay": arrival_delay,
                    "departure_time": departure_time,
                    "departure_delay": departure_delay,
                }
            )

        updates[trip_id] = {
            "trip_id": trip_id,
            "vehicle_id": trip_update.vehicle.id,
            "timestamp": trip_update.timestamp,
            "stop_updates": stop_updates,
        }

    return updates, feed.header.timestamp


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    print("=" * 75)
    print("SL TRIPUPDATES INSPECTION")
    print("=" * 75)

    # --------------------------------------------------
    # Fetch feeds
    # --------------------------------------------------

    print("\nFetching VehiclePositions...")

    vehicles, vehicle_feed_timestamp = (
        fetch_vehicle_positions()
    )

    print(
        f"VehiclePosition trips: {len(vehicles)}"
    )

    print("\nFetching TripUpdates...")

    trip_updates, trip_feed_timestamp = (
        fetch_trip_updates()
    )

    print(
        f"TripUpdates trips:     {len(trip_updates)}"
    )

    # --------------------------------------------------
    # Match feeds
    # --------------------------------------------------

    matching_trip_ids = (
        set(vehicles)
        & set(trip_updates)
    )

    print("\n" + "=" * 75)
    print("MATCH RESULT")
    print("=" * 75)

    print(
        f"VehiclePositions feed timestamp: "
        f"{vehicle_feed_timestamp}"
    )

    print(
        f"TripUpdates feed timestamp:      "
        f"{trip_feed_timestamp}"
    )

    print(
        f"Trips present in both feeds:     "
        f"{len(matching_trip_ids)}"
    )

    if vehicles:
        match_rate = (
            len(matching_trip_ids)
            / len(vehicles)
            * 100
        )

        print(
            f"Vehicle → TripUpdate match rate: "
            f"{match_rate:.1f}%"
        )

    # --------------------------------------------------
    # Inspect examples
    # --------------------------------------------------

    print("\n" + "=" * 75)
    print("EXAMPLE LIVE TRIPS")
    print("=" * 75)

    shown = 0

    for trip_id in matching_trip_ids:

        vehicle = vehicles[trip_id]
        update = trip_updates[trip_id]

        if not update["stop_updates"]:
            continue

        print("\n" + "-" * 75)

        print(
            f"Vehicle: {vehicle['vehicle_id']}"
        )

        print(
            f"Trip:    {trip_id}"
        )

        print(
            f"Vehicle timestamp: "
            f"{format_timestamp(vehicle['timestamp'])}"
        )

        print(
            f"TripUpdate timestamp: "
            f"{format_timestamp(update['timestamp'])}"
        )

        print(
            f"Position: "
            f"{vehicle['latitude']}, "
            f"{vehicle['longitude']}"
        )

        print(
            f"Stop updates: "
            f"{len(update['stop_updates'])}"
        )

        print("\nFirst 5 stop updates:")

        for stop in update["stop_updates"][:5]:

            print(
                f"\n  Stop sequence: {stop['stop_sequence']}"
            )

            print(
                f"  Stop ID:       {stop['stop_id']}"
            )

            print(
                f"  Arrival time:  "
                f"{format_timestamp(stop['arrival_time'])}"
            )

            print(
                f"  Arrival delay: "
                f"{stop['arrival_delay']}"
            )

            print(
                f"  Departure:     "
                f"{format_timestamp(stop['departure_time'])}"
            )

            print(
                f"  Depart delay:  "
                f"{stop['departure_delay']}"
            )

        shown += 1

        if shown >= 5:
            break


if __name__ == "__main__":
    main()