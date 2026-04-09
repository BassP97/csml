import time
import requests
import pandas as pd
from pathlib import Path
from typing import Optional

OUT_DIR = Path(__file__).parent / "light_rail_data"
OUT_FILE = OUT_DIR / "stop_ridership.csv"


def arcgis_query_all(
    base_url: str, where: str = "1=1", out_fields: str = "*"
) -> Optional[pd.DataFrame]:
    r = requests.get(
        base_url + "/query",
        params={"where": where, "returnCountOnly": "true", "f": "json"},
    )
    total = r.json().get("count", 0)
    if total == 0:
        return None

    records = []
    page_size = 1000
    offset = 0
    while offset < total:
        r2 = requests.get(
            base_url + "/query",
            params={
                "where": where,
                "outFields": out_fields,
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "f": "json",
                "returnGeometry": "true",
                "outSR": "4326",
            },
        )
        features = r2.json().get("features", [])
        if not features:
            break
        for f in features:
            row = f["attributes"]
            geom = f.get("geometry") or {}
            row["longitude"] = geom.get("x")
            row["latitude"] = geom.get("y")
            records.append(row)
        offset += len(features)
        time.sleep(0.1)

    return pd.DataFrame(records) if records else None


def fetch_mbta_green_line() -> Optional[pd.DataFrame]:
    dfs = []

    fall24 = arcgis_query_all(
        "https://services1.arcgis.com/ceiitspzDAHrdGO1/arcgis/rest/services/Fall_2024_MBTA_Rail_Ridership_by_Hour_Route_Line_and_Stop/FeatureServer/0",
        where="route_name='Green Line'",
    )
    if fall24 is not None:
        fall24["agency"] = "MBTA"
        fall24["dataset"] = "fall_2024_by_hour"

        total_service_days = (
            fall24.groupby("day_type_id")["number_service_days"].max().sum()
        )

        station_totals = (
            fall24.groupby("stop_name")
            .agg(total_ons=("total_ons", "sum"))
            .reset_index()
        )

        station_totals["avg_boardings_per_day"] = (
            station_totals["total_ons"] / total_service_days
        )

        average_boardings_by_day = station_totals.rename(
            columns={"stop_name": "station"}
        )
        average_boardings_by_day = average_boardings_by_day[
            ["station", "avg_boardings_per_day"]
        ]
        average_boardings_by_day["agency"] = "MBTA"
        average_boardings_by_day["dataset"] = "fall_2024_avg_by_day"
        dfs.append(average_boardings_by_day)
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def fetch_uta_light_rail() -> Optional[pd.DataFrame]:
    dfs = []
    for mode in ["Light Rail", "Streetcar"]:
        df = arcgis_query_all(
            "https://maps.rideuta.com/server/rest/services/Hosted/RAIL_STOP_RIDERSHIP_TABLE_SINCE_2017/FeatureServer/0",
            where=f"mode='{mode}'",
        )
        if df is not None:
            df["agency"] = "UTA"
            df["dataset"] = "monthly_since_2017"

            monthly = (
                df.groupby(["stopname", "month_", "year_"])["avgboardings"]
                .sum()
                .reset_index()
            )

            average_boardings_by_day = (
                monthly.groupby("stopname")["avgboardings"]
                .mean()
                .reset_index()
                .rename(
                    columns={
                        "stopname": "station",
                        "avgboardings": "avg_boardings_per_day",
                    }
                )
            )
            average_boardings_by_day["agency"] = "UTA"
            dfs.append(average_boardings_by_day)
    return pd.concat(dfs, ignore_index=True) if dfs else None


def fetch_nyc_ridership_data() -> Optional[pd.DataFrame]:
    r = requests.get(
        "https://data.ny.gov/resource/wujg-7c2s.json",
        params={
            "$select": "station_complex_id, station_complex, latitude, longitude, transit_timestamp, ridership",
            "$where": "transit_timestamp >= '2024-10-07T00:00:00' AND transit_timestamp < '2024-10-14T00:00:00'",
            "$limit": 50000,
        },
    )
    if r.status_code != 200:
        return None
    data = r.json()
    if not data:
        return None
    df = pd.DataFrame(data)
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["transit_timestamp"] = pd.to_datetime(df["transit_timestamp"])
    df["date"] = df["transit_timestamp"].dt.date

    daily = (
        df.groupby(["station_complex_id", "station_complex", "date"])
        .agg(
            ridership=("ridership", "sum"),
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
        )
        .reset_index()
    )
    result = (
        daily.groupby(["station_complex_id", "station_complex"])
        .agg(
            avg_boardings_per_day=("ridership", "mean"),
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
        )
        .reset_index()
        .rename(columns={"station_complex": "station"})
    )
    result["agency"] = "NYC Subway"
    return result[
        ["station", "avg_boardings_per_day", "latitude", "longitude", "agency"]
    ]


def fetch_chicago_ridership_data() -> Optional[pd.DataFrame]:
    r = requests.get(
        "https://data.cityofchicago.org/resource/5neh-572f.json",
        params={
            "$select": "stationname, avg(rides) AS avg_boardings_per_day",
            "$where": "date >= '2024-01-01T00:00:00' AND date < '2025-01-01T00:00:00'",
            "$group": "stationname",
            "$limit": 10000,
        },
    )
    if r.status_code != 200:
        return None
    data = r.json()
    if not data:
        return None
    df = pd.DataFrame(data)
    df["avg_boardings_per_day"] = pd.to_numeric(
        df["avg_boardings_per_day"], errors="coerce"
    )
    df = df.dropna(subset=["avg_boardings_per_day"])
    df = df.rename(columns={"stationname": "station"})
    df = df[["station", "avg_boardings_per_day"]]
    df["agency"] = "Chicago CTA"
    df["dataset"] = "daily_ridership_2024"
    return df


def fetch_portland_data() -> Optional[pd.DataFrame]:
    r = requests.get(
        "http://new.portal.its.pdx.edu:8080/transit/downloadquarterlydata?agency=trimet&quarter=2019-q3-summer"
    )
    if r.status_code != 200:
        return None
    data = r.text.splitlines()
    if not data:
        return None
    df = pd.DataFrame(data)
    df["agency"] = "Portland TriMet"
    df["dataset"] = "quarterly_ridership"
    print("schema: ", df.dtypes)
    return df


def main():
    OUT_DIR.mkdir(exist_ok=True)

    all_dfs = []

    mbta = fetch_mbta_green_line()
    if mbta is not None:
        all_dfs.append(mbta)
        print("Fetched MBTA Green Line data")
    else:
        print("Failed to fetch MBTA data")

    uta = fetch_uta_light_rail()
    if uta is not None:
        all_dfs.append(uta)
        print("Fetched UTA Light Rail data")
    else:
        print("Failed to fetch UTA data")

    nyc = fetch_nyc_ridership_data()
    if nyc is not None:
        all_dfs.append(nyc)
        print("Fetched NYC Subway data")
    else:
        print("Failed to fetch NYC Subway data")

    chicago = fetch_chicago_ridership_data()
    if chicago is not None:
        print("Fetched Chicago CTA data")
        all_dfs.append(chicago)
    else:
        print("Failed to fetch Chicago CTA data")

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.to_csv(OUT_FILE, index=False)


if __name__ == "__main__":
    main()
