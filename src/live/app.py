import io
import os
import time
import zipfile

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
print("=== APP MODULE STARTED ===", flush=True)

# --------------------------------------------------
# Page config
# --------------------------------------------------

st.set_page_config(
    page_title="Stockholm Transit Live",
    page_icon="🚇",
    layout="wide",
)


# --------------------------------------------------
# Config
# --------------------------------------------------

load_dotenv()

load_dotenv()

REALTIME_API_KEY = os.getenv("TRAFIKLAB_REALTIME_API_KEY")
STATIC_API_KEY = os.getenv("TRAFIKLAB_STATIC_API_KEY")

if not REALTIME_API_KEY:
    REALTIME_API_KEY = st.secrets.get("TRAFIKLAB_REALTIME_API_KEY")

if not STATIC_API_KEY:
    STATIC_API_KEY = st.secrets.get("TRAFIKLAB_STATIC_API_KEY")

if not REALTIME_API_KEY:
    st.error("TRAFIKLAB_REALTIME_API_KEY is missing")
    st.stop()

if not STATIC_API_KEY:
    st.error("TRAFIKLAB_STATIC_API_KEY is missing")
    st.stop()


VEHICLE_POSITIONS_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/VehiclePositionsSweden.pb"
)

TRIP_UPDATES_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-rt-sweden/sl/TripUpdatesSweden.pb"
)

STATIC_URL = (
    "https://opendata.samtrafiken.se/"
    "gtfs-sweden/sweden.zip"
)

STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")


# --------------------------------------------------
# Route type mapping
# --------------------------------------------------

ROUTE_TYPE_TO_MODE = {
    "0": "tram",
    "1": "subway",
    "2": "rail",
    "3": "bus",
    "4": "ferry",

    "100": "rail",
    "101": "rail",
    "102": "rail",
    "103": "rail",
    "105": "rail",
    "106": "rail",
    "109": "rail",

    "400": "subway",
    "401": "subway",
    "402": "subway",

    "700": "bus",
    "701": "bus",
    "702": "bus",
    "704": "bus",
    "705": "bus",
    "707": "bus",
    "710": "bus",
    "712": "bus",
    "715": "bus",

    "900": "tram",
    "901": "tram",
    "902": "tram",

    "1000": "ferry",
    "1200": "ferry",
}


MODE_COLORS = {
    "bus": [255, 99, 71],
    "subway": [0, 200, 255],
    "tram": [255, 215, 0],
    "ferry": [138, 43, 226],
    "rail": [50, 205, 50],
    "other": [200, 200, 200],
}


# rail intentionally excluded from V3 UI.
# Current SL VehiclePositions data contains rail vehicles
# without trip_id, preventing reliable route/trip matching.
VISIBLE_MODES = [
    "bus",
    "ferry",
    "subway",
    "tram",
]


# --------------------------------------------------
# Session state
# --------------------------------------------------

if "selected_vehicle_id" not in st.session_state:
    st.session_state.selected_vehicle_id = None

if "map_revision" not in st.session_state:
    st.session_state.map_revision = 0

if "known_sl_route_ids" not in st.session_state:
    st.session_state.known_sl_route_ids = set()

if "route_filter" not in st.session_state:
    st.session_state.route_filter = "__ALL__"


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def route_sort_key(value):
    value = str(value)

    numeric_part = ""
    text_part = ""

    for char in value:
        if char.isdigit() and not text_part:
            numeric_part += char
        else:
            text_part += char

    if numeric_part:
        return (
            0,
            int(numeric_part),
            text_part.lower(),
        )

    return (
        1,
        0,
        value.lower(),
    )


def clean_text(value):
    if pd.isna(value):
        return ""

    value = str(value).strip()

    if value.lower() == "nan":
        return ""

    return value


def format_clock(timestamp):
    if not timestamp:
        return ""

    return datetime.fromtimestamp(
        timestamp,
        tz=STOCKHOLM_TZ,
    ).strftime("%H:%M")


def format_delay(delay_seconds):
    if delay_seconds is None:
        return ""

    delay_minutes = round(delay_seconds / 60)

    if delay_minutes > 0:
        return f"{delay_minutes} min late"

    if delay_minutes < 0:
        return f"{abs(delay_minutes)} min early"

    return "On time"


def format_updated_ago(timestamp):
    if not timestamp:
        return "Unknown"

    seconds = max(
        0,
        int(time.time()) - int(timestamp),
    )

    if seconds < 60:
        return f"{seconds} sec ago"

    minutes = seconds // 60

    return f"{minutes} min ago"


def stop_following():
    st.session_state.selected_vehicle_id = None
    st.session_state.map_revision += 1


def select_route_from_search(route_id):
    st.session_state.route_filter = route_id


# --------------------------------------------------
# Static GTFS
# --------------------------------------------------

@st.cache_data(
    show_spinner="Loading Stockholm GTFS static data..."
)
def load_static_data():

    response = requests.get(
        STATIC_URL,
        params={"key": STATIC_API_KEY},
        timeout=120,
    )

    response.raise_for_status()

    with zipfile.ZipFile(
        io.BytesIO(response.content)
    ) as gtfs_zip:

        trips = pd.read_csv(
            gtfs_zip.open("trips.txt"),
            dtype=str,
            usecols=[
                "trip_id",
                "route_id",
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

        stop_times = pd.read_csv(
            gtfs_zip.open("stop_times.txt"),
            dtype={
                "trip_id": str,
                "stop_id": str,
                "stop_sequence": int,
            },
            usecols=[
                "trip_id",
                "stop_id",
                "stop_sequence",
            ],
        )

        stops = pd.read_csv(
            gtfs_zip.open("stops.txt"),
            dtype={
                "stop_id": str,
                "stop_name": str,
                "stop_lat": float,
                "stop_lon": float,
            },
            usecols=[
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
            ],
        )

    # --------------------------------------------------
    # Destination per trip
    # --------------------------------------------------

    final_stop_indexes = (
        stop_times
        .groupby("trip_id")["stop_sequence"]
        .idxmax()
    )

    final_stops = (
        stop_times
        .loc[
            final_stop_indexes,
            [
                "trip_id",
                "stop_id",
            ],
        ]
        .rename(
            columns={
                "stop_id": "destination_stop_id",
            }
        )
    )

    destination_names = (
        stops[
            [
                "stop_id",
                "stop_name",
            ]
        ]
        .rename(
            columns={
                "stop_id": "destination_stop_id",
                "stop_name": "destination_stop_name",
            }
        )
    )

    final_stops = final_stops.merge(
        destination_names,
        on="destination_stop_id",
        how="left",
    )

    lookup = trips.merge(
        routes,
        on="route_id",
        how="left",
    )

    lookup = lookup.merge(
        final_stops,
        on="trip_id",
        how="left",
    )

    lookup["route_id"] = (
        lookup["route_id"].astype(str)
    )

    lookup["transport_mode"] = (
        lookup["route_type"]
        .map(ROUTE_TYPE_TO_MODE)
        .fillna("other")
    )

    lookup["destination"] = (
        lookup["trip_headsign"]
        .fillna("")
        .str.strip()
    )

    missing_destination = (
        lookup["destination"] == ""
    )

    lookup.loc[
        missing_destination,
        "destination",
    ] = lookup.loc[
        missing_destination,
        "destination_stop_name",
    ]

    lookup["destination"] = (
        lookup["destination"]
        .fillna("Unknown destination")
    )

    # --------------------------------------------------
    # stop_id → stop_name
    # --------------------------------------------------

    stop_lookup = dict(
        zip(
            stops["stop_id"],
            stops["stop_name"],
        )
    )

    # --------------------------------------------------
    # Stops per trip
    # --------------------------------------------------

    trip_stops = stop_times.merge(
        stops[
            [
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
            ]
        ],
        on="stop_id",
        how="left",
    )

    trip_stops = (
        trip_stops
        .sort_values(
            [
                "trip_id",
                "stop_sequence",
            ]
        )
    )

    # --------------------------------------------------
    # Human-friendly route catalog
    # --------------------------------------------------

    route_rows = []

    for route_id, group in lookup.groupby(
        "route_id",
        dropna=False,
    ):

        if pd.isna(route_id):
            continue

        first = group.iloc[0]

        short_name = clean_text(
            first["route_short_name"]
        )

        long_name = clean_text(
            first["route_long_name"]
        )

        mode = clean_text(
            first["transport_mode"]
        )

        destinations = []

        for destination in group["destination"]:

            destination = clean_text(destination)

            if not destination:
                continue

            if destination == "Unknown destination":
                continue

            if destination not in destinations:
                destinations.append(destination)

        endpoint_text = " ↔ ".join(
            destinations[:2]
        )

        if mode in {
            "subway",
            "rail",
            "tram",
            "ferry",
        }:
            primary = (
                long_name
                or short_name
                or str(route_id)
            )

        else:
            primary = (
                short_name
                or long_name
                or str(route_id)
            )

        route_label = primary

        if endpoint_text:
            route_label = (
                f"{primary} — {endpoint_text}"
            )

        route_rows.append(
            {
                "route_id": str(route_id),
                "route_short_name": short_name,
                "route_long_name": long_name,
                "transport_mode": mode,
                "route_label": route_label,
            }
        )

    route_catalog = pd.DataFrame(route_rows)

    return (
        lookup,
        stop_lookup,
        trip_stops,
        route_catalog,
    )


# --------------------------------------------------
# VehiclePositions
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

        if not vehicle.HasField("position"):
            continue

        vehicles.append(
            {
                "vehicle_id": vehicle_id,
                "trip_id": trip_id,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "vehicle_timestamp": vehicle.timestamp,
            }
        )

    return (
        pd.DataFrame(vehicles),
        feed.header.timestamp,
    )


# --------------------------------------------------
# TripUpdates
# --------------------------------------------------

def fetch_trip_updates():

    response = requests.get(
        TRIP_UPDATES_URL,
        params={"key": REALTIME_API_KEY},
        timeout=30,
    )

    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    trip_updates = {}

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
                    arrival_time = (
                        stop_update.arrival.time
                    )

                if stop_update.arrival.HasField("delay"):
                    arrival_delay = (
                        stop_update.arrival.delay
                    )

            if stop_update.HasField("departure"):

                if stop_update.departure.time:
                    departure_time = (
                        stop_update.departure.time
                    )

                if stop_update.departure.HasField("delay"):
                    departure_delay = (
                        stop_update.departure.delay
                    )

            stop_updates.append(
                {
                    "stop_sequence":
                        stop_update.stop_sequence,
                    "stop_id":
                        stop_update.stop_id,
                    "arrival_time":
                        arrival_time,
                    "arrival_delay":
                        arrival_delay,
                    "departure_time":
                        departure_time,
                    "departure_delay":
                        departure_delay,
                }
            )

        trip_updates[trip_id] = stop_updates

    return (
        trip_updates,
        feed.header.timestamp,
    )


# --------------------------------------------------
# Next stop
# --------------------------------------------------

def get_next_stop(
    stop_updates,
    now_timestamp,
):

    if not stop_updates:
        return None

    candidates = []

    for stop in stop_updates:

        event_time = (
            stop["arrival_time"]
            or stop["departure_time"]
        )

        if not event_time:
            continue

        if event_time >= now_timestamp:
            candidates.append(
                (
                    event_time,
                    stop,
                )
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: item[0]
    )

    return candidates[0][1]


# --------------------------------------------------
# Static data
# --------------------------------------------------

print("=== STARTING STATIC GTFS LOAD ===", flush=True)

(
    static_lookup,
    stop_lookup,
    trip_stops,
    route_catalog,
) = load_static_data()

print("=== STATIC GTFS LOAD COMPLETE ===", flush=True)

# --------------------------------------------------
# Header
# --------------------------------------------------

st.title("Stockholm Transit Live")

st.caption(
    "Live SL vehicle positions with route, "
    "destination and realtime stop information."
)


# --------------------------------------------------
# Live fragment
# --------------------------------------------------

@st.fragment(run_every="10s")
def live_map():

    # --------------------------------------------------
    # Fetch realtime
    # --------------------------------------------------

    try:

        (
            vehicles,
            vehicle_feed_timestamp,
        ) = fetch_live_vehicles()

        (
            trip_updates,
            trip_feed_timestamp,
        ) = fetch_trip_updates()

    except requests.RequestException as exc:

        st.error(
            f"Failed to fetch realtime data: {exc}"
        )

        return

    if vehicles.empty:

        st.warning(
            "No live vehicles found."
        )

        return

    # --------------------------------------------------
    # Join live → static
    # --------------------------------------------------

    live = vehicles.merge(
        static_lookup,
        on="trip_id",
        how="left",
    )

    live = live.drop_duplicates(
        subset=["vehicle_id"],
        keep="first",
    )

    live["route_id"] = (
        live["route_id"]
        .astype("string")
    )

    live["route_short_name"] = (
        live["route_short_name"]
        .fillna("Unknown")
    )

    live["transport_mode"] = (
        live["transport_mode"]
        .fillna("other")
    )

    live["destination"] = (
        live["destination"]
        .fillna("Unknown destination")
    )


    # --------------------------------------------------
    # Remember routes observed in SL realtime
    # --------------------------------------------------

    active_sl_route_ids = set(
        live["route_id"]
        .dropna()
        .astype(str)
        .tolist()
    )

    st.session_state.known_sl_route_ids.update(
        active_sl_route_ids
    )

    sl_route_catalog = (
        route_catalog[
            route_catalog["route_id"].isin(
                st.session_state.known_sl_route_ids
            )
        ]
        .copy()
    )

    # --------------------------------------------------
    # Realtime next stop
    # --------------------------------------------------

    now_timestamp = int(time.time())

    next_stop_names = []
    next_stop_times = []
    next_stop_delays = []
    next_stop_ids = []
    realtime_statuses = []

    for row in live.itertuples():

        updates = trip_updates.get(
            row.trip_id
        )

        next_stop = get_next_stop(
            updates,
            now_timestamp,
        )

        if not next_stop:

            next_stop_names.append(
                "Live position available"
            )

            next_stop_times.append("")
            next_stop_delays.append("")
            next_stop_ids.append(None)

            realtime_statuses.append(
                "position_only"
            )

            continue

        stop_name = stop_lookup.get(
            next_stop["stop_id"],
            "Unknown stop",
        )

        event_time = (
            next_stop["arrival_time"]
            or next_stop["departure_time"]
        )

        delay = (
            next_stop["arrival_delay"]
            if next_stop["arrival_delay"] is not None
            else next_stop["departure_delay"]
        )

        next_stop_names.append(
            stop_name
        )

        next_stop_times.append(
            format_clock(event_time)
        )

        next_stop_delays.append(
            format_delay(delay)
        )

        next_stop_ids.append(
            str(next_stop["stop_id"])
        )

        realtime_statuses.append(
            "trip_update"
        )

    live["next_stop"] = next_stop_names
    live["next_time"] = next_stop_times
    live["delay_display"] = next_stop_delays
    live["next_stop_id"] = next_stop_ids
    live["realtime_status"] = realtime_statuses

    # --------------------------------------------------
    # Filters
    # --------------------------------------------------

    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:

        selected_mode = st.selectbox(
            "Transport mode",
            options=[
                "All",
                *VISIBLE_MODES,
            ],
            key="transport_mode_filter",
        )

    # --------------------------------------------------
    # Catalog for selected mode
    # --------------------------------------------------

    if selected_mode == "All":

        available_catalog = (
            sl_route_catalog[
                sl_route_catalog[
                    "transport_mode"
                ].isin(VISIBLE_MODES)
            ]
            .copy()
        )

        mode_filtered = (
            live[
                live[
                    "transport_mode"
                ].isin(VISIBLE_MODES)
            ]
            .copy()
        )

        all_routes_label = "All routes"

    else:

        available_catalog = (
            sl_route_catalog[
                sl_route_catalog[
                    "transport_mode"
                ] == selected_mode
            ]
            .copy()
        )

        mode_filtered = (
            live[
                live[
                    "transport_mode"
                ] == selected_mode
            ]
            .copy()
        )

        all_routes_label = (
            f"All {selected_mode} routes"
        )

    # --------------------------------------------------
    # Sort routes
    # --------------------------------------------------

    if not available_catalog.empty:

        available_catalog["_sort"] = (
            available_catalog[
                "route_short_name"
            ]
            .apply(route_sort_key)
        )

        available_catalog = (
            available_catalog
            .sort_values("_sort")
            .drop(columns="_sort")
        )

    route_label_lookup = dict(
        zip(
            available_catalog[
                "route_id"
            ],
            available_catalog[
                "route_label"
            ],
        )
    )

    route_options = [
        "__ALL__",
        *available_catalog[
            "route_id"
        ].tolist(),
    ]

    current_route = st.session_state.get(
        "route_filter",
        "__ALL__",
    )

    if current_route not in route_options:
        st.session_state.route_filter = "__ALL__"

    def route_format(route_id):

        if route_id == "__ALL__":
            return all_routes_label

        return route_label_lookup.get(
            route_id,
            route_id,
        )

    # --------------------------------------------------
    # Dropdown
    # --------------------------------------------------

    with filter_col2:

        selected_route_id = st.selectbox(
            "Route",
            options=route_options,
            format_func=route_format,
            key="route_filter",
        )

    # --------------------------------------------------
    # Independent route search
    # --------------------------------------------------

    search_query = st.text_input(
        "Search route",
        placeholder=(
            "Search line, destination or name "
            "— e.g. 116, Vällingby, Akalla"
        ),
        key="route_search",
    )

    search_query = (
        search_query
        .strip()
        .lower()
    )

    if search_query:

        search_results = (
            available_catalog[
                available_catalog[
                    "route_label"
                ]
                .fillna("")
                .str.lower()
                .str.contains(
                    search_query,
                    regex=False,
                )
            ]
            .head(8)
        )

        if search_results.empty:

            st.caption(
                "No matching routes."
            )

        else:

            st.caption(
                f"{len(search_results)} matching route"
                f"{'s' if len(search_results) != 1 else ''}"
            )

            for result in search_results.itertuples():

                st.button(
                    result.route_label,
                    key=(
                        "search_route_"
                        f"{result.route_id}"
                    ),
                    on_click=
                        select_route_from_search,
                    args=(
                        result.route_id,
                    ),
                    use_container_width=True,
                )

    # --------------------------------------------------
    # Apply route filter
    # --------------------------------------------------

    if selected_route_id == "__ALL__":

        filtered = mode_filtered.copy()

    else:

        filtered = (
            mode_filtered[
                mode_filtered[
                    "route_id"
                ].astype(str)
                == str(selected_route_id)
            ]
            .copy()
        )

    # --------------------------------------------------
    # Metrics
    # --------------------------------------------------

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Live vehicles",
        f"{len(filtered):,}",
    )

    col2.metric(
        "Routes active",
        filtered[
            "route_id"
        ].nunique(),
    )

    trip_update_match = (
        live[
            "realtime_status"
        ]
        .eq("trip_update")
        .mean()
        * 100
    )

    col3.metric(
        "Realtime status",
        f"{trip_update_match:.1f}%",
    )

    st.caption(
        f"Vehicle feed: "
        f"{vehicle_feed_timestamp} "
        f"· TripUpdates feed: "
        f"{trip_feed_timestamp}"
    )

    if filtered.empty:

        st.warning(
            "No live vehicles currently match "
            "the selected filters."
        )

        return

    # --------------------------------------------------
    # Vehicle display
    # --------------------------------------------------

    filtered["marker_color"] = (
        filtered[
            "transport_mode"
        ]
        .map(MODE_COLORS)
    )

    filtered["marker_color"] = (
        filtered[
            "marker_color"
        ]
        .apply(
            lambda value:
            value
            if isinstance(value, list)
            else MODE_COLORS["other"]
        )
    )

    filtered["mode_display"] = (
        filtered[
            "transport_mode"
        ]
        .str.capitalize()
    )

    filtered["next_line"] = (
        filtered["next_stop"]
        + " · "
        + filtered["next_time"]
    )

    no_time = (
        filtered["next_time"] == ""
    )

    filtered.loc[
        no_time,
        "next_line",
    ] = filtered.loc[
        no_time,
        "next_stop",
    ]

    # --------------------------------------------------
    # Tooltip
    # --------------------------------------------------

    filtered["tooltip_title"] = (
        filtered["route_short_name"]
        + " → "
        + filtered["destination"]
    )

    filtered["tooltip_subtitle"] = (
        filtered["mode_display"]
    )

    filtered["tooltip_detail"] = (
        "Next: "
        + filtered["next_line"]
    )

    has_delay = (
        filtered[
            "delay_display"
        ] != ""
    )

    filtered.loc[
        has_delay,
        "tooltip_detail",
    ] = (
        filtered.loc[
            has_delay,
            "tooltip_detail",
        ]
        + " · "
        + filtered.loc[
            has_delay,
            "delay_display",
        ]
    )

    # --------------------------------------------------
    # Followed vehicle
    # --------------------------------------------------

    selected_vehicle_id = (
        st.session_state[
            "selected_vehicle_id"
        ]
    )

    selected_live_rows = pd.DataFrame()

    if selected_vehicle_id:

        selected_live_rows = (
            live[
                live[
                    "vehicle_id"
                ] == selected_vehicle_id
            ]
        )

    filtered["marker_radius"] = 55

    if selected_vehicle_id:

        selected_mask = (
            filtered[
                "vehicle_id"
            ] == selected_vehicle_id
        )

        filtered.loc[
            selected_mask,
            "marker_radius",
        ] = 70

    # --------------------------------------------------
    # Vehicle layer
    # --------------------------------------------------

    vehicle_layer = pdk.Layer(
        "ScatterplotLayer",
        id="live-vehicles",
        data=filtered,
        get_position=[
            "longitude",
            "latitude",
        ],
        get_fill_color="marker_color",
        get_line_color=[
            255,
            255,
            255,
        ],
        get_radius="marker_radius",
        radius_min_pixels=4,
        radius_max_pixels=14,
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
        opacity=0.9,
        stroked=True,
        filled=True,
    )

    # --------------------------------------------------
    # Tracking radar
    # --------------------------------------------------

    pulse_layer = None

    if selected_vehicle_id:

        selected_marker_data = (
            filtered[
                filtered[
                    "vehicle_id"
                ] == selected_vehicle_id
            ]
            .copy()
        )

        if not selected_marker_data.empty:

            pulse_phase = (
                int(time.time()) % 4
            )

            pulse_radius = {
                0: 90,
                1: 120,
                2: 150,
                3: 180,
            }[
                pulse_phase
            ]

            pulse_layer = pdk.Layer(
                "ScatterplotLayer",
                id="selected-vehicle-pulse",
                data=selected_marker_data,
                get_position=[
                    "longitude",
                    "latitude",
                ],
                get_fill_color=[
                    0,
                    255,
                    120,
                    20,
                ],
                get_line_color=[
                    0,
                    255,
                    120,
                    220,
                ],
                get_radius=pulse_radius,
                radius_min_pixels=12,
                radius_max_pixels=30,
                line_width_min_pixels=2,
                stroked=True,
                filled=True,
                pickable=False,
            )

    # --------------------------------------------------
    # Selected trip stops
    # --------------------------------------------------

    stop_layer = None
    next_stop_layer = None

    if not selected_live_rows.empty:

        followed_vehicle = (
            selected_live_rows.iloc[0]
        )

        followed_trip_id = str(
            followed_vehicle[
                "trip_id"
            ]
        )

        followed_next_stop_id = (
            followed_vehicle[
                "next_stop_id"
            ]
        )

        selected_trip_stops = (
            trip_stops[
                trip_stops[
                    "trip_id"
                ] == followed_trip_id
            ]
            .copy()
        )

        selected_trip_stops = (
            selected_trip_stops
            .dropna(
                subset=[
                    "stop_lat",
                    "stop_lon",
                ]
            )
        )

        if not selected_trip_stops.empty:

            selected_trip_stops[
                "tooltip_title"
            ] = (
                selected_trip_stops[
                    "stop_name"
                ]
                .fillna(
                    "Unknown stop"
                )
            )

            selected_trip_stops[
                "tooltip_subtitle"
            ] = "Stop"

            selected_trip_stops[
                "tooltip_detail"
            ] = ""

            stop_layer = pdk.Layer(
                "ScatterplotLayer",
                id="selected-trip-stops",
                data=selected_trip_stops,
                get_position=[
                    "stop_lon",
                    "stop_lat",
                ],
                get_fill_color=[
                    220,
                    220,
                    220,
                    150,
                ],
                get_line_color=[
                    255,
                    255,
                    255,
                    220,
                ],
                get_radius=35,
                radius_min_pixels=3,
                radius_max_pixels=7,
                line_width_min_pixels=1,
                stroked=True,
                filled=True,
                pickable=True,
            )

            if followed_next_stop_id:

                next_stop_data = (
                    selected_trip_stops[
                        selected_trip_stops[
                            "stop_id"
                        ]
                        .astype(str)
                        == str(
                            followed_next_stop_id
                        )
                    ]
                    .copy()
                )

                if not next_stop_data.empty:

                    next_stop_data[
                        "tooltip_subtitle"
                    ] = "Next stop"

                    next_stop_data[
                        "tooltip_detail"
                    ] = (
                        followed_vehicle[
                            "next_time"
                        ]
                    )

                    if followed_vehicle[
                        "delay_display"
                    ]:

                        next_stop_data[
                            "tooltip_detail"
                        ] = (
                            next_stop_data[
                                "tooltip_detail"
                            ]
                            + " · "
                            + followed_vehicle[
                                "delay_display"
                            ]
                        )

                    next_stop_layer = pdk.Layer(
                        "ScatterplotLayer",
                        id="selected-next-stop",
                        data=next_stop_data,
                        get_position=[
                            "stop_lon",
                            "stop_lat",
                        ],
                        get_fill_color=[
                            0,
                            255,
                            120,
                            100,
                        ],
                        get_line_color=[
                            0,
                            255,
                            120,
                            255,
                        ],
                        get_radius=80,
                        radius_min_pixels=8,
                        radius_max_pixels=15,
                        line_width_min_pixels=3,
                        stroked=True,
                        filled=True,
                        pickable=True,
                    )

    # --------------------------------------------------
    # Map view
    # --------------------------------------------------

    if not selected_live_rows.empty:

        followed_vehicle = (
            selected_live_rows.iloc[0]
        )

        view_state = pdk.ViewState(
            latitude=float(
                followed_vehicle[
                    "latitude"
                ]
            ),
            longitude=float(
                followed_vehicle[
                    "longitude"
                ]
            ),
            zoom=14,
            pitch=0,
        )

    else:

        view_state = pdk.ViewState(
            latitude=59.3293,
            longitude=18.0686,
            zoom=9,
            pitch=0,
        )

    # --------------------------------------------------
    # Tooltip
    # --------------------------------------------------

    tooltip = {
        "html": """
            <div style="
                font-size: 14px;
                min-width: 190px;
            ">

                <div style="
                    font-size: 17px;
                    font-weight: 700;
                    margin-bottom: 4px;
                ">
                    {tooltip_title}
                </div>

                <div style="
                    color: #b8b8b8;
                ">
                    {tooltip_subtitle}
                </div>

                <div style="
                    margin-top: 8px;
                    font-size: 13px;
                ">
                    {tooltip_detail}
                </div>

            </div>
        """,
        "style": {
            "backgroundColor":
                "rgba(15, 18, 24, 0.96)",
            "color": "white",
            "borderRadius": "8px",
            "padding": "10px",
        },
    }

    # --------------------------------------------------
    # Layers
    # --------------------------------------------------

    map_layers = []

    if stop_layer is not None:
        map_layers.append(stop_layer)

    if next_stop_layer is not None:
        map_layers.append(next_stop_layer)

    if pulse_layer is not None:
        map_layers.append(pulse_layer)

    map_layers.append(vehicle_layer)

    deck = pdk.Deck(
        layers=map_layers,
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style=None,
    )

    # --------------------------------------------------
    # Map selection
    # --------------------------------------------------

    event = st.pydeck_chart(
        deck,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-object",
        key=(
            "live_vehicle_map_"
            f"{st.session_state.map_revision}"
        ),
    )

    # --------------------------------------------------
    # Save clicked vehicle
    # --------------------------------------------------

    if (
        st.session_state[
            "selected_vehicle_id"
        ]
        is None
    ):

        try:

            selected_objects = (
                event
                .selection
                .objects
                .get(
                    "live-vehicles",
                    [],
                )
            )

        except (
            AttributeError,
            KeyError,
        ):

            selected_objects = []

        if selected_objects:

            clicked_vehicle_id = str(
                selected_objects[0][
                    "vehicle_id"
                ]
            )

            st.session_state[
                "selected_vehicle_id"
            ] = clicked_vehicle_id

            st.rerun()

    # --------------------------------------------------
    # Following panel
    # --------------------------------------------------

    selected_vehicle_id = (
        st.session_state[
            "selected_vehicle_id"
        ]
    )

    if not selected_vehicle_id:

        st.caption(
            "Click a vehicle on the map "
            "to follow it."
        )

        return

    selected_rows = (
        live[
            live[
                "vehicle_id"
            ] == selected_vehicle_id
        ]
    )

    if selected_rows.empty:

        st.warning(
            "The selected vehicle is "
            "temporarily missing from "
            "the live feed."
        )

        if st.button(
            "Stop following",
            key="stop_follow_missing",
        ):

            stop_following()
            st.rerun()

        return

    selected = selected_rows.iloc[0]

    # --------------------------------------------------
    # Vehicle panel
    # --------------------------------------------------

    st.divider()

    panel_title, panel_button = (
        st.columns([5, 1])
    )

    with panel_title:

        st.subheader(
            "Following vehicle"
        )

    with panel_button:

        if st.button(
            "Stop following",
            key="stop_follow",
            use_container_width=True,
        ):

            stop_following()
            st.rerun()

    st.markdown(
        f"### "
        f"{selected['route_short_name']} "
        f"→ "
        f"{selected['destination']}"
    )

    st.caption(
        str(
            selected[
                "transport_mode"
            ]
        ).capitalize()
    )

    (
        info_col1,
        info_col2,
        info_col3,
    ) = st.columns(3)

    with info_col1:

        st.caption(
            "NEXT STOP"
        )

        st.markdown(
            f"**"
            f"{selected['next_stop']}"
            f"**"
        )

    with info_col2:

        st.caption(
            "ESTIMATED"
        )

        st.markdown(
            f"**"
            f"{selected['next_time'] or '—'}"
            f"**"
        )

    with info_col3:

        st.caption(
            "STATUS"
        )

        status = (
            selected[
                "delay_display"
            ]
            or "Position only"
        )

        st.markdown(
            f"**{status}**"
        )

    st.markdown(
        f":red[●] Live · updated "
        f"{format_updated_ago(
            selected[
                'vehicle_timestamp'
            ]
        )}"
    )


live_map()