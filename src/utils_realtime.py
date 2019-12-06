from tempfile import TemporaryFile
from datetime import datetime
import requests
import zipfile
import csv
import re
import io

ALERT_FLAGS = {"autobusy", "tramwaje", "skm", "kolej", "metro"}

def no_html(text):
    "Clean text from html tags"
    if text == "None": return ""
    else: return re.sub("<.*?>", "", text)

def alert_flags(alert_soup, alert_type):
    "Get additional flags about the alert from icons, passed as BS4's soup"

    flags = set()

    # Selector is different depending on the alert type
    if alert_type == 2:
        icons = alert_soup.find_all("img", class_="impediment-category-icon")

    else:
        icons = alert_soup.find_all("img", class_="format-icon")

    # Icons → Flags
    for icon in icons:
        flags.add(icon.get("alt"))

    return flags.intersection(ALERT_FLAGS)

def alert_data(rss_item, alert_type):
    data = {}

    data["id"] = f"A/{alert_type.upper()}/" + re.search(r"(?<=p=)\d+", rss_item.find("guid").text)[0]
    data["title"] = no_html(rss_item.find("description").text)
    data["link"] = no_html(rss_item.find("link").text)
    data["body"] = rss_item.find("{http://purl.org/rss/1.0/modules/content/}encoded").text
    data["htmlbody"] = data["body"]

    # Effect
    if alert_type == "impediment":
        data["effect"] = 2

    elif alert_type == "change":
        data["effect"] = 7

    else:
        raise ValueError("unknown alert_type " + repr(alert_type))

    # Parse affected routes
    lines_raw = rss_item.find("title").text.upper()

    if lines_raw.startswith("UTRUDNIENIA W KOMUNIKACJI: "):
        lines_raw = lines_raw[26:]

    elif lines_raw.startswith("ZMIANY W KOMUNIKACJI: "):
        lines_raw = lines_raw[22:]

    else:
        lines_raw = ""

    data["routes"] = re.findall(r"[0-9A-Za-z-]{1,3}", lines_raw)

    return data

def alert_description(alert_soup, alert_type):
    "Get alert description from BS4's soup. Returns a (plain_text, html) for every alert soup"
    # Different selector based on alert type
    if alert_type == 2:
        alert_soup = alert_soup.find("div", class_="impediment-content")

    else:
        alert_soup = alert_soup.find("main", class_="page-main")

        if not alert_soup:
            return "", ""

        for i in alert_soup.find_all("div", class_="is-style-small"):
            i.decompose()

    # Get what's left over
    desc_with_tags = str(alert_soup)

    # Remove everything after <hr>
    desc_with_tags = re.sub(r"<hr\s?/?>(?!.*<hr\s?/?>).*", "", desc_with_tags, flags=re.DOTALL)

    # Clean text from HTML tags
    clean_desc = no_html(
        desc_with_tags              \
            .replace("</p>", "\n")  \
            .replace("<br/>", "\n") \
            .replace("<br>", "\n")  \
            .replace("\xa0", " ")   \
            .replace("  "," ")      \
    ).strip()

    return clean_desc, desc_with_tags

def timepoint_in_trips(timepoint, route, stop, times):
    "Try find trip_id in times for given timepoint, route and stop"
    valid_times = [i for i in times if i["routeId"] == route and i["stopId"] == stop]
    valid_trips = [i for i in times if i["timepoint"] == timepoint]

    # If not found, try to add 24h to timepoint, to catch after-midnight trips
    if not valid_trips:
        next_timepoint = ":".join([str(int(timepoint.split(":")[0]) + 24), timepoint.split(":")[1], timepoint.split(":")[2]])
        valid_trips = [i for i in times if i["timepoint"] == next_timepoint]

    if valid_trips:
        return valid_trips[0]["tripId"]

def later_in_time(t1, t2):
    "Check if t2 happended after t1. Both should be strings HH:MM:SS."
    t1 = [int(x) for x in t1.split(":")]
    t2 = [int(x) for x in t2.split(":")]

    # Fix for after-midnight trips
    if t2[0] >= 24 and t1[0] <= 3:
        t1[0] += 24

    t1 = 3600*t1[0] + 60*t1[1] + t1[2]
    t2 = 3600*t2[0] + 60*t2[1] + t2[2]

    return t2 > t1

def parse_apium_response(api_response):
    """Parses a wierd response from api.um.warszawa.pl, they kinda seem to overcomplicate JSON"""
    result = []

    for item in api_response["result"]:

        item_dict = {}

        for kv_pair in item["values"]:

            # Each item has to have a 'key' and 'value' keys
            if "key" not in kv_pair or "value" not in kv_pair:
                continue

            # Convert "null" string to None
            # Beacuse why use JSON's null, when you can use a "null" string
            if kv_pair["value"] == "null":
                kv_pair["value"] = None

            item_dict[kv_pair["key"]] = kv_pair["value"]

        result.append(item_dict)

    return result

def load_api_positions(apikey, request_type):
    api_response = requests.get(
        "https://api.um.warszawa.pl/api/action/busestrams_get/",
        timeout = 5,
        params = {
            "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
            "apikey": apikey,
            "type": request_type,
    })
    api_response.raise_for_status()
    api_response = api_response.json()

    # Check if response from API UM is correct, and add it to positions list
    if type(api_response["result"]) is list:
        return api_response["result"]
    elif api_response.get("error") == "Błędny apikey lub jego brak":
        print("WarsawGTFS-RT: Incorrect apikey!")
    elif request_type == "1":
        print("WarsawGTFS-RT: Incorrect buses positions response")
        print(api_response)
    elif request_type == "2":
        print("WarsawGTFS-RT: Incorrect trams positions response")
        print(api_response)

class WarsawGtfs:
    def __init__(self, gtfs_location):
        self.routes = {"0": set(), "1": set(), "2": set(), "3": set()}
        self.stops = {}
        self.services = set()

        if gtfs_location.startswith("https://") or gtfs_location.startswith("ftp://") or gtfs_location.startswith("http://"):
            gtfs_request = requests.get(gtfs_location)
            self.gtfs = TemporaryFile()
            self.gtfs.write(gtfs_request.content)
            self.gtfs.seek(0)

        else:
            self.gtfs = open(gtfs_location, mode="rb")

        self.arch = zipfile.ZipFile(self.gtfs, mode="r")

    @classmethod
    def routes_only(cls, gtfs_location):
        self = cls(gtfs_location)
        self.list_routes()
        self.close()

        return self.routes

    def list_routes(self):
        with self.arch.open("routes.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                if row["route_type"] not in self.routes: continue
                else: self.routes[row["route_type"]].add(row["route_id"])

    def list_services(self):
        today = datetime.today().strftime("%Y%m%d")

        with self.arch.open("calendar_dates.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                if row["date"] == today: self.services.add(row["service_id"])

    def list_stops(self):
        with self.arch.open("stops.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                self.stops[row["stop_id"]] = [row["stop_lat"], row["stop_lon"]] # list, not tuple because of json module

    def list(self):
        self.list_stops()
        self.list_routes()
        self.list_services()

    def close(self):
        self.arch.close()
        self.gtfs.close()
