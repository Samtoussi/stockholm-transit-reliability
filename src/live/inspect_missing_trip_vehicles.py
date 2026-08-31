import os

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


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


response = requests.get(
    VEHICLE_POSITIONS_URL,
    params={"key": API_KEY},
    timeout=30,
)

response.raise_for_status()


feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)


total_vehicles = 0
with_trip = 0
without_trip = 0

missing_trip_examples = []


for entity in feed.entity:

    if not entity.HasField("vehicle"):
        continue

    vehicle = entity.vehicle

    total_vehicles += 1

    trip_id = vehicle.trip.trip_id

    if trip_id:
        with_trip += 1
        continue

    without_trip += 1

    if len(missing_trip_examples) < 20:

        missing_trip_examples.append(
            {
                "vehicle_id": vehicle.vehicle.id,
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
                "stop_id": vehicle.stop_id,
                "current_status": vehicle.current_status,
            }
        )


print()
print("=== SL VehiclePositions inspection ===")
print()

print(f"Feed timestamp: {feed.header.timestamp}")
print(f"Total vehicles: {total_vehicles}")
print(f"With trip_id: {with_trip}")
print(f"Without trip_id: {without_trip}")

if total_vehicles:

    pct_missing = (
        without_trip
        / total_vehicles
        * 100
    )

    print(
        f"Missing trip_id: {pct_missing:.1f}%"
    )


print()
print("=== Examples without trip_id ===")
print()

for i, vehicle in enumerate(
    missing_trip_examples,
    start=1,
):

    print(f"Example #{i}")

    print(
        f"  vehicle_id: "
        f"{vehicle['vehicle_id']}"
    )

    print(
        f"  position: "
        f"{vehicle['latitude']}, "
        f"{vehicle['longitude']}"
    )

    print(
        f"  timestamp: "
        f"{vehicle['timestamp']}"
    )

    print(
        f"  stop_id: "
        f"{vehicle['stop_id']}"
    )

    print(
        f"  current_status: "
        f"{vehicle['current_status']}"
    )

    print()