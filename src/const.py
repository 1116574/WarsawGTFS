HEADERS = {
    "agency.txt": [
        "agency_id", "agency_name", "agency_url", "agency_timezone",
        "agency_lang", "agency_phone", "agency_fare_url",
    ],

    "attributions.txt": [
        "attribution_id", "organization_name", "is_producer", "is_operator",
        "is_authority", "is_data_source", "attribution_url",
    ],

    "feed_info.txt": ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"],

    "calendar_dates.txt": ["service_id", "date", "exception_type"],

    "shapes.txt": [
        "shape_id", "shape_pt_sequence", "shape_dist_traveled", "shape_pt_lat", "shape_pt_lon"
    ],

    "stops.txt": [
        "stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station",
        "zone_id", "stop_IBNR", "stop_PKPPLK", "platform_code", "wheelchair_boarding",
    ],

    "routes.txt": [
        "agency_id", "route_id", "route_short_name", "route_long_name", "route_type",
        "route_color", "route_text_color", "route_sort_order",
    ],

    "trips.txt": [
        "route_id", "service_id", "trip_id", "trip_headsign", "direction_id",
        "shape_id", "exceptional", "wheelchair_accessible", "bikes_allowed",
    ],

    "stop_times.txt": [
        "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
        "pickup_type", "drop_off_type", "shape_dist_traveled",
    ],
}

# List of rail stops used by S× lines. Other rail stops are ignored.
ACTIVE_RAIL_STATIONS = {
    "4900", "4901", "7900", "7901", "7902", "2901", "2900", "2918", "2917", "2916", "2915",
    "2909", "2908", "2907", "2906", "2905", "2904", "2903", "2902", "4902", "4903", "4923",
    "4904", "4905", "2914", "2913", "2912", "2911", "2910", "4919", "3901", "4918", "4917",
    "4913", "1910", "1909", "1908", "1907", "1906", "1905", "1904", "1903", "1902", "1901",
    "7903", "5908", "5907", "5904", "5903", "5902", "1913", "1914", "1915",
}

PROPER_STOP_NAMES = {
    "4040": "Lotnisko Chopina",              "1484": "Dom Samotnej Matki",
    "2005": "Praga-Płd. - Ratusz",           "1541": "Marki Bandurskiego I",
    "5001": "Połczyńska - Parking P+R",      "2296": "Szosa Lubelska",
    "6201": "Lipków Paschalisa-Jakubowicza", "1226": "Mańki-Wojody",
    "2324": "Wiązowna",
}
