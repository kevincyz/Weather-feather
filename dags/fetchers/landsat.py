def fetch_landsat_data(location, date_range):
    return {
        "type": "LANDSAT",
        "location": location,
        "date_range": date_range,
        "bands": {"B1": 1234, "B2": 5678}
    }
