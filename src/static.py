from collections import OrderedDict
from tempfile import NamedTemporaryFile
from typing import AbstractSet, Mapping, Optional
from warnings import warn
from datetime import date, datetime, timedelta
from ftplib import FTP
import libarchive.public
import requests
import zipfile
import json
import csv
import re
import os

from .const import HEADERS, ACTIVE_RAIL_STATIONS, PROPER_STOP_NAMES

from .utils_static import route_color_type, normal_stop_name, normal_time, \
    should_town_be_added_to_name, proper_headsign, trip_direction, match_day_type, \
    Shaper, Metro

from .utils import clear_directory, avg_position

from .parser import Parser

class StopHandler:
    def __init__(self):
        # Stop data
        self.data = OrderedDict()
        self.names = PROPER_STOP_NAMES.copy()
        self.parents = {}
        self.zones = {}

        # Invalid stop data
        self.invalid = {}
        self.change = {}

        # Used stops
        self.used_invalid = set()
        self.used = set()

        # External data
        print("\033[1A\033[K" + "Loading external data about stops")

        self.missing_stops = requests.get(
            "https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/"
            "raw/missing_stops.json").json()

        self.rail_platforms = requests.get(
            "https://gist.githubusercontent.com/MKuranowski/0ca97a012d541899cb1f859cd0bab2e7/"
            "raw/rail_platforms.json").json()

    @staticmethod
    def _match_virtual(virt: dict, stakes: list) -> Optional[str]:
        """Try to find a normal stake corresponding to given virtual stake"""
        # Find normal stakes with matching position
        if virt["lat"] is not None and virt["lon"] is not None:
            with_same_pos = [i["id"] for i in stakes if i["code"][0] != "8"
                             and i["lat"] == virt["lat"] and i["lon"] == virt["lon"]]
        else:
            with_same_pos = []

        # Find normal stakes with matching code
        with_same_code = [i["id"] for i in stakes if i["code"][0] != "8"
                          and i["lat"] == virt["lat"] and i["lon"] == virt["lon"]]

        # Special Case: Metro Młociny 88 → Metro Młociny 28
        if virt["id"] == "605988" and "605928" in with_same_code:
            return "605928"

        # Matched stakes with the same position
        if with_same_pos:
            return with_same_pos[0]

        # Matched stakes with the same code
        elif with_same_code:
            return with_same_code[0]

        # Unable to find a match
        else:
            return None

    def _find_missing_positions(self, stakes: list) -> list:
        for idx, stake in enumerate(stakes):

            if stake["lat"] is None or stake["lon"] is None:
                missing_pos = self.missing_stops.get(stake["id"])

                if missing_pos:
                    stakes[idx]["lat"], stakes[idx]["lon"] = missing_pos

        return stakes

    def _load_normal_group(self, group_name, stakes):
        for stake in stakes:

            # Fix virtual stops
            if stake["code"][0] == "8":
                change_to = self._match_virtual(stake, stakes)

                if change_to is not None:
                    self.change[stake["id"]] = change_to

                else:
                    self.invalid[stake["id"]] = stake

                continue

            # Handle undefined stop positions
            if stake["lat"] is None or stake["lon"] is None:
                self.invalid[stake["id"]] = stake
                continue

            # Save stake into self.data
            self.data[stake["id"]] = {
                "stop_id": stake["id"],
                "stop_name": f'{group_name} {stake["code"]}',
                "stop_lat": stake["lat"],
                "stop_lon": stake["lon"],
                "wheelchair_boarding": stake["wheelchair"],
            }

    def _load_railway_group(self, group_id, group_name, stakes):
        # Nop KM & WKD stations
        if group_id not in ACTIVE_RAIL_STATIONS:
            for i in stakes:
                self.change[i["id"]] = None
            return

        # Load station info
        station_data = self.rail_platforms.get(group_id, {})

        # If this station is not in rail_platforms, average all stake positions
        # In order to calculate an approx position of the station
        if not station_data:
            stake_positions = [(i["lat"], i["lon"]) for i in stakes if
                               i["lat"] is not None and i["lon"] is not None]

            if stake_positions:
                station_lat, station_lon = avg_position(stake_positions)

            # Halt processing if we have no geographical data
            else:
                for i in stakes:
                    self.change[i["id"]] = None
                return

        # Otherwise get the position from rail_platforms data
        else:
            station_lat, station_lon = map(float, station_data["pos"].split(","))
            group_name = station_data["name"]

        # Map every stake into one node
        if (not station_data) or station_data["oneplatform"]:

            self.data[group_id] = {
                "stop_id": group_id,
                "stop_name": group_name,
                "stop_lat": station_lat,
                "stop_lon": station_lon,
                "zone_id": station_data.get("zone_id", ""),
                "stop_IBNR": station_data.get("ibnr_code", ""),
                "stop_PKPPLK": station_data.get("pkpplk_code", ""),
                "wheelchair_boarding": station_data.get("wheelchair", "0"),
            }

            for i in stakes:
                self.change[i["id"]] = group_id

        # Process multi-platform station
        else:
            # Add hub entry
            self.data[group_id] = {
                "stop_id": group_id,
                "stop_name": group_name,
                "stop_lat": station_lat,
                "stop_lon": station_lon,
                "location_type": "1",
                "parent_station": "",
                "zone_id": station_data.get("zone_id", ""),
                "stop_IBNR": station_data.get("ibnr_code", ""),
                "stop_PKPPLK": station_data.get("pkpplk_code", ""),
                "wheelchair_boarding": station_data.get("wheelchair", "0"),
            }

            # Platforms
            for platform_id, platform_pos in station_data["platforms"].items():
                platform_lat, platform_lon = map(float, platform_pos.split(","))
                platform_code = platform_id.split("p")[1]
                platform_name = f"{group_name} peron {platform_code}"

                # Add platform entry
                self.data[platform_id] = {
                    "stop_id": platform_id,
                    "stop_name": platform_name,
                    "stop_lat": platform_lat,
                    "stop_lon": platform_lon,
                    "location_type": "0",
                    "parent_station": group_id,
                    "zone_id": station_data.get("zone_id", ""),
                    "stop_IBNR": station_data.get("ibnr_code", ""),
                    "stop_PKPPLK": station_data.get("pkpplk_code", ""),
                    "wheelchair_boarding": station_data.get("wheelchair", "0"),
                }

                # Add to self.parents
                self.parents[platform_id] = group_id

            # Stops → Platforms
            for stake in stakes:

                # Defined stake in rail_platforms
                if stake["id"] in station_data["stops"]:
                    self.change[stake["id"]] = station_data["stops"][stake["id"]]

                # Unknown stake
                elif stake["id"] not in {"491303", "491304"}:
                    warn(f'No platform defined for railway PR entry {group_name} {stake["id"]}')

    def load_group(self, group_info, stakes):
        # Fix name "Kampinoski Pn" town name
        if group_info["town"] == "Kampinoski Pn":
            group_info["town"] = "Kampinoski PN"

        # Fix group name
        group_info["name"] = normal_stop_name(group_info["name"])

        # Add town name to stop name & save name to self.names
        if group_info["id"] in self.names:
            group_info["name"] = self.names[group_info["id"]]

        elif should_town_be_added_to_name(group_info):
            group_info["name"] = f'{group_info["town"]} {group_info["name"]}'
            self.names[group_info["id"]] = group_info["name"]

        else:
            self.names[group_info["id"]] = group_info["name"]

        # Add missing positions to stakes
        stakes = self._find_missing_positions(stakes)

        # Parse stakes
        if group_info["id"][1:3] in {"90", "91", "92"}:
            self._load_railway_group(group_info["id"], group_info["name"], stakes)

        else:
            self._load_normal_group(group_info["name"], stakes)

    def get_id(self, original_id: str) -> Optional[str]:
        """Should the stop_id be changed, provide the correct stop_id.
        If given stop_id has its position undefined returns None.
        """
        valid_id = self.change.get(original_id, original_id)

        if valid_id is None or valid_id in self.invalid:
            self.used_invalid.add(valid_id)
            return None

        else:
            return valid_id

    def use(self, stop_id: str) -> None:
        """Mark provided GTFS stop_id as used"""
        # Check if this stop belogins to a larger group
        parent_id = self.parents.get(stop_id)

        # Mark the parent as used
        if parent_id is not None:
            self.used.add(parent_id)

        self.used.add(stop_id)

    def zone_set(self, group_id, zone_id):
        current_zone = self.zones.get(group_id)

        # Zone has not changed: skip
        if current_zone == zone_id:
            return

        if current_zone is None:
            self.zones[group_id] = zone_id

        # Boundary stops shouldn't generate a zone conflict warning
        elif current_zone == "1/2" or zone_id == "1/2":
            self.zones[group_id] = "1/2"

        else:

            warn(f"Stop group {group_id} has a zone confict: it was set to {current_zone!r}, "
                 f"but now it needs to be set to {zone_id!r}")

            self.zones[group_id] = "1/2"

    def export(self):
        print("\033[1A\033[K" + "Exporting stops")

        # Export all stops
        with open("gtfs/stops.txt", mode="w", encoding="utf8", newline="") as f:
            writer = csv.DictWriter(f, HEADERS["stops.txt"])
            writer.writeheader()

            for stop_id, stop_data in self.data.items():
                # Check if stop was used or (is a part of station and not a stop-chlid)
                if stop_id in self.used or (stop_data.get("parent_station") in self.used
                                            and stop_data.get("location_type") != "1"):

                    # Set the zone_id
                    if not stop_data.get("zone_id"):
                        zone_id = self.zones.get(stop_id[:4])

                        if zone_id is None:
                            warn(f"Stop group {stop_id[:4]} has no zone_id assigned (using '1/2')")
                            zone_id = "1/2"

                        stop_data["zone_id"] = zone_id

                    writer.writerow(stop_data)

        # Calculate unused stos from missing
        unused_missing = set(self.missing_stops.keys()).difference(self.used_invalid)

        # Dump missing stops info
        with open("missing_stops.json", "w") as f:
            json.dump({"missing": sorted(self.used_invalid), "unused": sorted(unused_missing)},
                      f, indent=2)

class Converter:
    def __init__(self, shapes=False, clear_shape_errors=True):
        clear_directory("gtfs")

        if clear_shape_errors:
            clear_directory("shape-errors")

        # Data handlers
        self.stops = StopHandler()
        self.calendars = {}

        # File handler
        self.version = None
        self.reader = None
        self.parser = None

        # Get shape generator instance
        if isinstance(shapes, Shaper):
            self.shapes = shapes
            self.shapes.open()

        elif shapes:
            self.shapes = Shaper()
            self.shapes.open()

        else:
            self.shapes = None

    def open_files(self):
        self.file_routes = open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="")
        self.wrtr_routes = csv.DictWriter(self.file_routes, HEADERS["routes.txt"])
        self.wrtr_routes.writeheader()

        self.file_trips = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        self.wrtr_trips = csv.DictWriter(self.file_trips, HEADERS["trips.txt"])
        self.wrtr_trips.writeheader()

        self.file_times = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        self.wrtr_times = csv.DictWriter(self.file_times, HEADERS["stop_times.txt"])
        self.wrtr_times.writeheader()

        self.file_dates = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        self.wrtr_dates = csv.DictWriter(self.file_dates, HEADERS["calendar_dates.txt"])
        self.wrtr_dates.writeheader()

    def close_files(self):
        self.file_routes.close()
        self.file_trips.close()
        self.file_times.close()
        self.file_dates.close()

    def get_file(self, version):
        """Download and decompress schedules for current data.
        Returns tuple (TemporaryFile, version) -
        and that TemporaryFile is decompressed .txt file
        """

        # Login to ZTM server and get the list of files
        server = FTP("rozklady.ztm.waw.pl")
        server.login()
        files = [f for f in server.nlst() if re.fullmatch(r"RA\d{6}\.7z", f)]

        # If user has requested an exact version, check if it's on the server
        if version:
            fname = "{}.7z".format(version)
            if fname not in files:
                raise KeyError(f"Requested file version ({version}) not found on ZTM server")

        # If not, find one valid today
        else:
            fdate = date.today()
            while True:
                fname = fdate.strftime("RA%y%m%d.7z")
                if fname in files:
                    break
                else:
                    fdate -= timedelta(days=1)

        # Create temporary files for storing th 7z archive and the compressed TXT file
        temp_arch = NamedTemporaryFile(mode="w+b", delete=False)
        self.reader = NamedTemporaryFile(mode="w+t", delete=True)

        try:
            # Download the file
            server.retrbinary("RETR " + fname, temp_arch.write)
            server.quit()
            temp_arch.close()

            # Open the temporary archive inside
            with libarchive.public.file_reader(temp_arch.name) as arch:

                # Iterate over each file inside the archive
                for arch_file in arch:
                    name = arch_file.pathname.upper()

                    # Assert the file inside the archive is the TXT file we're looking for
                    if not name.startswith("RA") or not name.endswith(".TXT"):
                        continue

                    # Save the feed version
                    self.version = name[:8]

                    # Decompress the TXT file block by block and save it to the reader
                    for block in arch_file.get_blocks():
                        self.reader.write(str(block, "cp1250"))
                    self.reader.seek(0)

                    # only one TXT file should be inside the archive
                    break

                else:
                    raise FileNotFoundError(f"no schedule file found inside archive {fname!r}")

        # Remove the temp arch file at the end
        finally:
            os.remove(temp_arch.name)

        self.parser = Parser(self.reader)

    def get_calendars(self):
        print("\033[1A\033[K" + "Loading calendars (KA)")
        this_month = date.today().replace(day=1)

        for day in self.parser.parse_ka():
            if day["date"] < this_month:
                continue

            self.calendars[day["date"]] = day["services"]

    def get_stops(self):
        print("\033[1A\033[K" + "Loading stops (ZP)")

        for group in self.parser.parse_zp():
            stakes = list(self.parser.parse_pr())
            self.stops.load_group(group, stakes)

    def get_schedules(self):
        route_sort_order = 1  # Leave first 2 blank for M1 and M2 routes
        route_id = None

        print("\033[1A\033[K" + "Parsing schedules (LL)")

        for route in self.parser.parse_ll():
            route_id, route_desc = route["id"], route["desc"]

            # Ignore Koleje Mazowieckie & Warszawska Kolej Dojazdowa routes
            if route_id.startswith("R") or route_id.startswith("WKD"):
                self.parser.skip_to_section("WK", end=True)
                continue

            print("\033[1A\033[K" + f"Parsing routes & schedules (LL) - {route_id}")

            route_sort_order += 1
            route_type, route_color, route_text_color = route_color_type(route_id, route_desc)

            # Data loaded from TR section
            route_name = ""
            direction_stops = {"0": set(), "1": set()}
            on_demand_stops = set()
            inaccesible_trips = set()
            used_day_types = set()
            variant_directions = {}

            # Variants
            print("\033[1A\033[K" + f"Parsing routes & schedules (TR) - {route_id}")

            for variant in self.parser.parse_tr():
                print("\033[1A\033[K" + f"Parsing routes & schedules (LW) - {route_id}")

                stops = list(self.parser.parse_lw())

                # add zones
                for stop in stops:
                    self.stops.zone_set(stop["id"][:4], stop["zone"])

                # variant direction
                variant_directions[variant["id"]] = variant["direction"]

                # route_name should be the name of first and last stop of 1st variant
                if not route_name:
                    route_name = " — ".join([
                        self.stops.names[stops[0]["id"][:4]],
                        self.stops.names[stops[-1]["id"][:4]]
                    ])

                # add on_demand_stops from this variant
                on_demand_stops |= {i["id"] for i in stops if i["on_demand"]}

                # add stop_ids to proper direction in direction_stops
                direction_stops[variant["direction"]] |= {i["id"] for i in stops}

                # now parse ODWG sections - for inaccesible trips (only tram)
                if route_type == "0":
                    print("\033[1A\033[K" + f"Parsing routes & schedules (TD) - {route_id}")

                    for trip in self.parser.parse_wgod(route_type, route_id):
                        if not trip["accessible"]:
                            inaccesible_trips.add(trip["id"])

                else:
                    self.parser.skip_to_section("RP", end=True)

            # Schedules
            print("\033[1A\033[K" + f"Parsing routes & schedules (WK) - {route_id}")

            for trip in self.parser.parse_wk(route_id):

                # Change stop_ids based on stops_map
                for stopt in trip["stops"]:
                    stopt["orig_stop"] = stopt.pop("stop")
                    stopt["stop"] = self.stops.get_id(stopt["orig_stop"])

                # Fliter "None" stops
                trip["stops"] = [i for i in trip["stops"] if i["stop"]]

                # Ignore trips with only 1 stopt
                if len(trip["stops"]) < 2:
                    continue

                # Unpack info from trip_id
                trip_id = trip["id"]

                trip_id_split = trip_id.split("/")
                variant_id = trip_id_split[1]

                day_type = trip_id_split[2]
                service_id = route_id + "/" + day_type

                del trip_id_split

                # "Exceptional" trip - a deutor/depot run
                if variant_id.startswith("TP-") or variant_id.startswith("TO-"):
                    exceptional = "0"
                else:
                    exceptional = "1"

                # Shapes
                if self.shapes:
                    stop_data_list = [
                        (
                            i["stop"],
                            self.stops.data[i["stop"]]["stop_lat"],
                            self.stops.data[i["stop"]]["stop_lon"],
                        )
                        for i in trip["stops"]
                    ]

                    shape_id, shape_distances = self.shapes.get(
                        route_type, trip_id, stop_data_list
                    )

                else:
                    shape_id, shape_distances = "", {}

                # Wheelchair Accessibility
                if trip_id in inaccesible_trips:
                    wheelchair = "2"
                else:
                    wheelchair = "1"

                # Direction
                if variant_id in variant_directions:
                    direction = variant_directions[variant_id]
                else:
                    direction = trip_direction(
                        {i["orig_stop"] for i in trip["stops"]},
                        direction_stops)

                    variant_directions[variant_id] = direction

                # Headsign
                last_stop = trip["stops"][-1]["stop"]
                headsign = proper_headsign(
                    last_stop,
                    self.stops.names.get(last_stop[:4], "")
                )

                if not headsign:
                    warn(f"No headsign for trip {trip_id}")

                # day type
                used_day_types.add(day_type)

                # Write to trips.txt
                self.wrtr_trips.writerow({
                    "route_id": route_id,
                    "service_id": service_id,
                    "trip_id": trip_id,
                    "trip_headsign": headsign,
                    "direction_id": direction,
                    "shape_id": shape_id,
                    "exceptional": exceptional,
                    "wheelchair_accessible": wheelchair,
                    "bikes_allowed": "1",
                })

                max_seq = len(trip["stops"]) - 1

                # StopTimes
                for seq, stopt in enumerate(trip["stops"]):
                    # Pickup Type
                    if seq == max_seq:
                        pickup = "1"
                    elif "P" in stopt["flags"]:
                        pickup = "1"
                    elif stopt["orig_stop"] in on_demand_stops:
                        pickup = "3"
                    else:
                        pickup = "0"

                    # Drop-Off Type
                    if seq == 0:
                        dropoff = "1"
                    elif stopt["orig_stop"] in on_demand_stops:
                        dropoff = "3"
                    else:
                        dropoff = "0"

                    # Shape Distance
                    stop_dist = shape_distances.get(seq, "")

                    if stop_dist:
                        stop_dist = round(stop_dist, 4)

                    # Mark stop as used
                    self.stops.use(stopt["stop"])

                    # Output to stop_times.txt
                    self.wrtr_times.writerow({
                        "trip_id": trip_id,
                        "arrival_time": stopt["time"],
                        "departure_time": stopt["time"],
                        "stop_id": stopt["stop"],
                        "stop_sequence": seq,
                        "pickup_type": pickup,
                        "drop_off_type": dropoff,
                        "shape_dist_traveled": stop_dist,
                    })

            # Services
            for day, possible_day_types in self.calendars.items():
                active_day_type = match_day_type(used_day_types, possible_day_types)
                if active_day_type:
                    self.wrtr_dates.writerow({
                        "service_id": route_id + "/" + active_day_type,
                        "date": day.strftime("%Y%m%d"),
                        "exception_type": "1",
                    })

            # Output to routes.txt
            self.wrtr_routes.writerow({
                "agency_id": "0",
                "route_id": route_id,
                "route_short_name": route_id,
                "route_long_name": route_name,
                "route_type": route_type,
                "route_color": route_color,
                "route_text_color": route_text_color,
                "route_sort_order": route_sort_order,
            })

    def parse(self):
        self.get_calendars()
        self.get_stops()
        self.get_schedules()
        self.stops.export()

    @staticmethod
    def static_files(shapes, version, download_time):
        "Create files that don't depend of ZTM file content"
        buff = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
        buff.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,'
                   'agency_fare_url\n')

        buff.write('0,"Warszawski Transport Publiczny","https://www.wtp.waw.pl",Europe/Warsaw,pl,'
                   '19 115,"https://www.wtp.waw.pl/ceny-i-rodzaje-biletow/"\n')
        buff.close()

        buff = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n")
        buff.write('feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n')
        buff.write('"WarsawGTFS (provided by Mikołaj Kuranowski)",'
                   f'"https://github.com/MKuranowski/WarsawGTFS",pl,{version}\n')
        buff.close()

        buff = open("gtfs/attributions.txt", mode="w", encoding="utf8", newline="\r\n")
        buff.write('attribution_id,organization_name,is_producer,is_operator,is_authority,'
                   'is_data_source,attribution_url\n')
        buff.write('0,"WarsawGTFS (provided by Mikołaj Kuranowski)",pl,1,0,0,'
                   '0,"https://github.com/MKuranowski/WarsawGTFS"\n')

        buff.write(f'1,"ZTM Warszawa (retrieved {download_time})",pl,0,0,1,'
                   '1,"https://ztm.waw.pl"\n')

        if shapes:
            buff.write('2,"Bus shapes (under ODbL licnese): © OpenStreetMap contributors",pl,'
                       '0,0,1,1,"https://www.openstreetmap.org/copyright"\n')

        buff.close()

    @staticmethod
    def compress(target="gtfs.zip"):
        "Compress all created files to gtfs.zip"
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for entry in os.scandir("gtfs"):
                if entry.name.endswith(".txt"):
                    archive.write(entry.path, arcname=entry.name)

    @classmethod
    def create(cls, version="", shapes=False, metro=False, prevver="", targetfile="gtfs.zip",
               clear_shape_errors=True):

        self = cls(shapes)

        print("\033[1A\033[K" + "Downloading file")
        download_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_file(version)

        if prevver == self.version:
            self.reader.close()
            print("\033[1A\033[K" + "File matches the 'prevver' argument, aborting!")
            return

        self.open_files()

        try:
            print("\033[1A\033[K" + "Starting parser...")
            self.parse()

        finally:

            self.close_files()
            if shapes:
                self.shapes.close()

        print("\033[1A\033[K" + "Parser finished working, closing TXT file")
        self.reader.close()

        print("\033[1A\033[K" + "Creating static files")
        self.static_files(bool(self.shapes), self.version, download_time)

        if metro:
            print("\033[1A\033[K" + "Adding metro")
            Metro.add()

        print("\033[1A\033[K" + "Compressing")
        self.compress(targetfile)

        return self.version
