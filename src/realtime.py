from google.transit import gtfs_realtime_pb2 as gtfs_rt
from collections import OrderedDict
from datetime import datetime, timedelta
from tempfile import TemporaryFile
from bs4 import BeautifulSoup
from urllib import request
from lxml import etree
from copy import copy
import requests
import sqlite3
import zipfile
import math
import json
import csv
import re
import os
import io

from .utils_realtime import alert_flags, alert_data, alert_description, later_in_time, \
    parse_apium_response, load_api_positions, WarsawGtfs

from .utils import haversine, initial_bearing

class Realtime:

    @staticmethod
    def alerts(gtfs_location="https://mkuran.pl/gtfs/warsaw.zip", out_proto=True,
               binary_proto=True, out_json=False):
        "Get ZTM Warszawa Alerts"
        # Grab Entries
        changes_req = requests.get("https://www.wtp.waw.pl/feed/?post_type=change")
        impediments_req = requests.get("https://www.wtp.waw.pl/feed/?post_type=impediment")
        gtfs_routes = WarsawGtfs.routes_only(gtfs_location)

        # Get Alerts into etrees
        changes_req.encoding = "utf-8"
        changes = etree.fromstring(changes_req.content)

        impediments_req.encoding = "utf-8"
        impediments = etree.fromstring(impediments_req.content)

        # Containers
        if out_proto:
            container = gtfs_rt.FeedMessage()
            header = container.header
            header.gtfs_realtime_version = "2.0"
            header.incrementality = 0
            header.timestamp = round(datetime.today().timestamp())

        if out_json:
            json_container = {
                "time": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "alerts": []
            }

        # Sort Entries
        all_entries = []
        for i in impediments.findall("channel/item"):
            data = alert_data(i, alert_type="impediment")
            data["effect"] = 2  # Reduced Service

            all_entries.append(data)

        for i in changes.findall("channel/item"):
            data = alert_data(i, alert_type="change")
            data["effect"] = 7  # Other Effect

            all_entries.append(data)

        # Alerts
        for entry in all_entries:
            # Additional info from website provided by RSS
            alert_website = requests.get(entry["link"])
            alert_website.raise_for_status()
            alert_website.encoding = "utf-8"

            soup = BeautifulSoup(alert_website.text, "html.parser")

            # Get routes for this alert
            entry["routes"] = [i for i in entry["routes"] if
                               i in gtfs_routes["0"]
                               or i in gtfs_routes["1"]
                               or i in gtfs_routes["2"]
                               or i in gtfs_routes["3"]]

            # Add routes if those are not specified
            if not entry["routes"]:
                flags = alert_flags(soup, entry["effect"])

                if "metro" in flags:
                    entry["routes"].extend(gtfs_routes["1"])

                elif "tramwaje" in flags:
                    entry["routes"].extend(gtfs_routes["0"])

                elif flags.intersection("kolej", "skm"):
                    entry["routes"].extend(gtfs_routes["2"])

                elif "autobusy" in flags:
                    entry["routes"].extend(gtfs_routes["3"])

            desc, desc_html = alert_description(soup, entry["effect"])

            if desc and desc_html:
                entry["body"] = desc
                entry["htmlbody"] = desc_html

            else:
                # print(f'Unable to scrap page {entry["link"]}')
                entry["body"] = ""
                entry["htmlbody"] = ""

            if entry["routes"]:

                # Append to gtfs_rt container
                if out_proto:
                    entity = container.entity.add()
                    entity.id = entry["id"]

                    alert = entity.alert
                    alert.effect = entry["effect"]
                    alert.url.translation.add().text = entry["link"]
                    alert.header_text.translation.add().text = entry["title"]

                    if desc:
                        alert.description_text.translation.add().text = entry["body"]

                    for route in entry["routes"]:
                        selector = alert.informed_entity.add()
                        selector.route_id = route

                # Append to JSON container
                if out_json:
                    json_entry = OrderedDict()
                    json_entry["id"] = entry["id"]
                    json_entry["routes"] = entry["routes"]

                    json_entry["effect"] = \
                        "REDUCED_SERVICE" if entry["effect"] == 2 \
                        else "OTHER_EFFECT"

                    json_entry["link"] = entry["link"]
                    json_entry["title"] = entry["title"]
                    json_entry["body"] = entry["body"]
                    json_entry["htmlbody"] = entry["htmlbody"]

                    json_container["alerts"].append(json_entry)

        # Export
        if out_proto and binary_proto:
            with open("gtfs-rt/alerts.pb", "wb") as f:
                f.write(container.SerializeToString())

        elif out_proto:
            with open("gtfs-rt/alerts.pb", "w") as f:
                f.write(str(container))

        if out_json:
            with open("gtfs-rt/alerts.json", "w", encoding="utf8") as f:
                json.dump(json_container, f, indent=2, ensure_ascii=False)

    @staticmethod
    def brigades(apikey, gtfs_location="https://mkuran.pl/gtfs/warsaw.zip", export=False):
        "Create a brigades table to match positions to gtfs"
        # Variables
        brigades = {}

        trip_last_points = {}
        api_responses = {}
        matched_trips = set()

        # Download GTFS
        print("Retreaving GTFS")
        gtfs = WarsawGtfs(gtfs_location)

        print("Reading routes, services and stops from GTFS")
        gtfs.list_all()

        # We need only route_ids of trams and buses — other are not needed for brigades
        gtfs.routes = gtfs.routes["0"] | gtfs.routes["3"]

        print("Matching stop_times.txt to brigades", end="\n\n")

        # And now open stop_times and match trip_id with brigade,
        # by matching route_id+stop_id+departure_time with api.um.warszawa.pl schedules,
        # which have brigade number

        with gtfs.arch.open("stop_times.txt", mode="r") as stoptimes:
            reader = csv.DictReader(io.TextIOWrapper(stoptimes, encoding="utf8", newline=""))

            for row in reader:

                trip_id = row["trip_id"]
                route_id, service_id = gtfs.trips.get(trip_id)

                # Ignore nonactive routes & services
                if route_id not in gtfs.routes or service_id not in gtfs.services:
                    continue

                # Other info about stop_time
                stop_id = row["stop_id"]
                stop_index = int(row["stop_sequence"])
                timepoint = row["departure_time"]

                print("\033[1A\033[K"
                      + "Next stop_time row: "
                      + f"T: {trip_id} I: {stop_index} ({timepoint})")

                # If considered timepoint of a trip happens »later«,
                # then what's stored in trip_last_points => overwrite last stop of trip
                if trip_last_points.get(trip_id, {}).get("index", -1) < stop_index:
                    trip_last_points[trip_id] = {
                        "stop": stop_id,
                        "index": stop_index,
                        "timepoint": timepoint
                    }

                # If there's no brigade for this trip, try to match it
                if trip_id not in matched_trips:
                    if (route_id, stop_id) not in api_responses:

                        try:
                            print("\033[1A\033[K" + "Making new API call: "
                                  + f"R: {route_id} S: {stop_id}")

                            api_request = requests.get(
                                "https://api.um.warszawa.pl/api/action/dbtimetable_get/",
                                timeout=5,
                                params={
                                    "id": "e923fa0e-d96c-43f9-ae6e-60518c9f3238",
                                    "apikey": apikey,
                                    "busstopId": stop_id[:4],
                                    "busstopNr": stop_id[4:6],
                                    "line": route_id
                                }
                            )

                            api_request.raise_for_status()
                            print("\033[1A\033[K" + "Reading recived API response for: "
                                  + f"R: {route_id} S: {stop_id}")
                            api_response = api_request.json()

                            if not isinstance(api_response["result"], list):
                                raise ValueError("api result is not a list!")

                            result = parse_apium_response(api_response)

                        except requests.exceptions.Timeout:
                            print(
                                "\033[1A\033[K\033[1m"
                                f"Incorrent API response for R: {route_id} S: {stop_id} "
                                "| TIMEOUT\033[0m",
                                end="\n\n"
                            )
                            continue

                        except requests.exceptions.HTTPError:
                            print(
                                "\033[1A\033[K\033[1m"
                                f"Incorrent API response for R: {route_id} S: {stop_id} "
                                f"| {api_request.status_code}\033[0m",
                                end="\n\n"
                            )
                            continue

                        except (json.decoder.JSONDecodeError, AssertionError):
                            print(
                                "\033[1A\033[K\033[1m"
                                f"Incorrent API response for R: {route_id} S: {stop_id}:\033[0m\n"
                                f"{api_request.text!r}",
                                end="\n\n"
                            )
                            continue

                        api_responses[(route_id, stop_id)] = result

                    else:
                        result = api_responses[(route_id, stop_id)]

                    for departure in result:
                        if departure.get("czas") == timepoint:
                            brigade_id = departure.get("brygada", "").lstrip("0")
                            break
                    else:
                        brigade_id = ""

                    if not brigade_id:
                        continue

                    matched_trips.add(trip_id)

                    if route_id not in brigades:
                        brigades[route_id] = {}

                    if brigade_id not in brigades[route_id]:
                        brigades[route_id][brigade_id] = []

                    brigades[route_id][brigade_id].append({"trip_id": trip_id})

        gtfs.close()

        print("\033[1A\033[K" + "Matching stop_times.txt to brigades: done")

        # Sort everything
        print("Appending info about last timepoint to brigade")
        for route in brigades:
            for brigade in brigades[route]:
                brigades[route][brigade] = sorted(brigades[route][brigade],
                                                  key=lambda i: i["trip_id"].split("/")[-1])

                for trip in brigades[route][brigade]:
                    trip_last_point = trip_last_points[trip["trip_id"]]
                    trip["last_stop_id"] = trip_last_point["stop"]
                    trip["last_stop_latlon"] = gtfs.stops[trip_last_point["stop"]]
                    trip["last_stop_timepoint"] = trip_last_point["timepoint"]

            brigades[route] = OrderedDict(sorted(brigades[route].items()))

        if export:
            print("Exporting")
            with open("gtfs-rt/brigades.json", "w") as jsonfile:
                jsonfile.write(json.dumps(brigades, indent=2))

        return brigades

    @staticmethod
    def positions(apikey, brigades="https://mkuran.pl/gtfs/warsaw/brigades.json", previous={},
                  out_proto=True, binary_proto=True, out_json=False):
        "Get ZTM Warszawa positions"
        # Variables
        positions = OrderedDict()
        source = []

        # GTFS-RT Container
        if out_proto:
            container = gtfs_rt.FeedMessage()
            header = container.header
            header.gtfs_realtime_version = "2.0"
            header.incrementality = 0
            header.timestamp = round(datetime.today().timestamp())

        # JSON Container
        if out_json:
            json_container = OrderedDict()
            json_container["time"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            json_container["positions"] = []

        # Get brigades, if brigades is not already a dict or OrderedDict
        if type(brigades) is str:
            if brigades.startswith("ftp://") or brigades.startswith("http://") \
                    or brigades.startswith("https://"):
                brigades_request = requests.get(brigades)
                brigades = brigades_request.json()
            else:
                with open(brigades) as f:
                    brigades = json.load(f)

        # Sort times in brigades, if they're not sorted
        if type(brigades) is not OrderedDict:
            for route in brigades:
                for brigade in brigades[route]:
                    brigades[route][brigade] = sorted(
                        brigades[route][brigade],
                        key=lambda i: i["trip_id"].split("/")[-1]
                    )

        # Load data from API UM
        source += load_api_positions(apikey, "1")  # Bus posiions
        source += load_api_positions(apikey, "2")  # Tram positions

        # Iterate over results
        for v in source:
            # Read data about position
            lat, lon, route, brigade = v["Lat"], v["Lon"], v["Lines"], v["Brigade"].lstrip("0")
            tstamp = datetime.strptime(v["Time"], "%Y-%m-%d %H:%M:%S")
            trip_id = None
            bearing = None
            vehicle_id = "/".join(["V", route, brigade])
            triplist = brigades.get(route, {}).get(brigade, [])

            if not triplist:
                continue

            # Do not care about obsolete data
            if (datetime.today() - tstamp) > timedelta(minutes=10):
                continue

            # Try to match with trip based on the difference between vehicle positons
            previous_veh_data = previous.get(vehicle_id)

            if previous_veh_data is None:
                prev_trip = previous_veh_data["trip_id"]
                prev_lat = previous_veh_data["lat"]
                prev_lon = previous_veh_data["lon"]
                prev_bearing = previous[vehicle_id].get("bearing", None)

                tripidslist = [x["trip_id"] for x in triplist]

                # Get vehicle bearing
                bearing = initial_bearing([prev_lat, prev_lon], [lat, lon])
                if (not bearing) and prev_bearing:
                    bearing = prev_bearing

                # If vehicle was doing its last trip, there's nothing more that can be calculated
                if prev_trip == triplist[-1]["trip_id"]:
                    trip_id = prev_trip

                # The calculations require for the prev_trip to be in the triplist
                elif prev_trip in tripidslist:
                    prev_trip_index = tripidslist.index(prev_trip)
                    prev_trip_last_latlon = [float(i) for i in
                                             triplist[prev_trip_index]["last_stop_latlon"]]

                    trip_near_terminus = haversine([lat, lon], prev_trip_last_latlon) <= 0.05
                    trip_shouldve_finished = later_in_time(
                        triplist[prev_trip_index]["last_stop_timepoint"],
                        (datetime.now() - timedelta(minutes=30)).strftime("%H:%M:%S")
                    )

                    # If vehicle is near (50m) the last stop => the trip has finished
                    # Or if the previous trip should've finished 30min earlier (fallback)
                    # FIXME: Some trips pass around last stop more then one time (see 146/TP-FAL-W)
                    if trip_near_terminus or trip_shouldve_finished:
                        trip_id = triplist[prev_trip_index + 1]["trip_id"]
                    else:
                        trip_id = prev_trip

            # If this vehicle wasn't defined previously we have to assume it's running on time
            if not trip_id:
                currtime = datetime.now().strftime("%H:%M:%S")
                for trip in triplist:
                    if later_in_time(currtime, trip["last_stop_timepoint"]):
                        trip_id = trip["trip_id"]
                        break

            # If the vehicle has no active trips now - assume it's doing the last trip
            if not trip_id:
                trip_id = triplist[-1]["trip_id"]

            # Save to dict
            data = {
                "id": vehicle_id,
                "trip_id": trip_id,
                "timestamp": tstamp,
                "lat": lat,
                "lon": lon
            }

            if bearing:
                data["bearing"] = bearing

            positions[vehicle_id] = data

            # Save to gtfs_rt container
            if out_proto:
                entity = container.entity.add()
                entity.id = vehicle_id

                vehicle = entity.vehicle
                vehicle.timestamp = round(tstamp.timestamp())
                vehicle.trip.trip_id = trip_id
                vehicle.vehicle.id = vehicle_id

                vehicle.position.latitude = float(lat)
                vehicle.position.longitude = float(lon)
                if bearing:
                    vehicle.position.bearing = float(bearing)

        # Export results
        if out_proto and binary_proto:
            with open("gtfs-rt/vehicles.pb", "wb") as f:
                f.write(container.SerializeToString())

        elif out_proto:
            with open("gtfs-rt/vehicles.pb", "w") as f:
                f.write(str(container))

        if out_json:
            for i in map(copy, positions.values()):
                i["timestamp"] = i["timestamp"].isoformat()
                json_container["positions"].append(i)

            with open("gtfs-rt/vehicles.json", "w", encoding="utf8") as f:
                json.dump(json_container, f, indent=2, ensure_ascii=False)

        return positions
