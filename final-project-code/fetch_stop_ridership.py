from dataclasses import dataclass
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Optional
import censusgeocode as cg

OUT_DIR = Path(__file__).parent / "light_rail_data"
OUT_FILE = OUT_DIR / "stop_ridership.csv"


@dataclass
class CensusData:
    population: int
    median_income: int
    percent_under_5: Optional[float] = None
    pct_over_85: Optional[float] = None
    median_household_income: Optional[int] = None
    per_capita_income: Optional[int] = None
    gini_index: Optional[float] = None
    median_home_value: Optional[int] = None
    percent_below_poverty: Optional[float] = None
    median_rent: Optional[int] = None
    pct_bachelors_or_higher: Optional[float] = None
    pct_labor_force_unemployed: Optional[float] = None
    pct_married: Optional[float] = None
    pct_with_health_insurance: Optional[float] = None
    pct_foreign_born: Optional[float] = None
    pct_renter_occupied: Optional[float] = None
    pct_with_computer: Optional[float] = None
    pct_with_internet: Optional[float] = None
    pct_no_vehicle_available: Optional[float] = None


def get_census_data_from_coordinates(
    latitude: float, longitude: float
) -> Optional[CensusData]:
    try:
        result = cg.coordinates(x=longitude, y=latitude)
        if not result or "Census Tracts" not in result or not result["Census Tracts"]:
            return None
        tract_info = result["Census Tracts"][0]
        url = "https://api.census.gov/data/2022/acs/acs5"
        params = {
            "get": ",".join(
                [
                    "B01003_001E",  # total population
                    "B01001_001E",  # sex-by-age
                    "B01001_003E",  # male under 5
                    "B01001_027E",  # female under 5
                    "B01001_025E",  # male 85+
                    "B01001_049E",  # female 85+
                    "B19013_001E",  # median household income
                    "B19301_001E",  # per capita income
                    "B19083_001E",  # gini index
                    "B25077_001E",  # median home value
                    "B25064_001E",  # median gross rent
                    "B25003_001E",  # occupied housing units
                    "B25003_003E",  # renter occupied
                    "B17001_001E",  # poverty
                    "B17001_002E",  # below poverty level
                    "B15003_001E",  # educational attainment
                    "B15003_022E",  # bachelor's degree
                    "B15003_023E",  # master's degree
                    "B15003_024E",  # professional school degree
                    "B15003_025E",  # doctorate degree
                    "B23025_002E",  # in civilian labor force
                    "B23025_005E",  # civilian unemployed
                    "B12001_001E",  # marital status
                    "B12001_004E",  # male, now married
                    "B12001_013E",  # female, now married
                    "B27010_001E",  # health insurance
                    "B27010_017E",  # under 19, uninsured
                    "B27010_033E",  # 19–34, uninsured
                    "B27010_050E",  # 35–64, uninsured
                    "B27010_066E",  # 65+, uninsured
                    "B05002_001E",  # nativity
                    "B05002_013E",  # foreign born
                    "B28001_001E",  # computer
                    "B28001_002E",  # has a computer
                    "B28002_001E",  # internet
                    "B28002_013E",  # no internet access
                    "B08201_001E",  # vehicle availability
                    "B08201_002E",  # no vehicle available
                ]
            ),
            "for": f"tract:{tract_info['TRACT']}",
            "in": f"state:{tract_info['STATE']} county:{tract_info['COUNTY']}",
            "key": "",
        }
        response = requests.get(url, params=params)
        print(response.json())
        data = response.json()
        if len(data) < 2:
            print(f"No census data found for coordinates ({latitude}, {longitude})")
            return None
        d = data[1]  # shorthand

        def _denom(val):
            v = int(val)
            return None if v <= 0 else v

        pop = _denom(d[0])
        housing = _denom(d[11])
        poverty = _denom(d[13])
        education = _denom(d[15])
        labor = _denom(d[20])
        marital = _denom(d[22])
        health = _denom(d[25])
        nativity = _denom(d[30])
        computer = _denom(d[32])
        internet = _denom(d[34])
        vehicle = _denom(d[36])

        ret = CensusData(
            population=int(d[0]),
            median_income=int(d[6]),
            percent_under_5=(int(d[2]) + int(d[3])) / pop * 100
            if pop is not None
            else None,
            pct_over_85=(int(d[4]) + int(d[5])) / pop * 100
            if pop is not None
            else None,
            median_household_income=int(d[6]),
            per_capita_income=int(d[7]),
            gini_index=float(d[8]),
            median_home_value=int(d[9]),
            median_rent=int(d[10]),
            pct_renter_occupied=int(d[12]) / housing * 100
            if housing is not None
            else None,
            percent_below_poverty=int(d[14]) / poverty * 100
            if poverty is not None
            else None,
            pct_bachelors_or_higher=(
                (int(d[16]) + int(d[17]) + int(d[18]) + int(d[19])) / education * 100
                if education is not None
                else None
            ),
            pct_labor_force_unemployed=int(d[21]) / labor * 100
            if labor is not None
            else None,
            pct_married=(int(d[23]) + int(d[24])) / marital * 100
            if marital is not None
            else None,
            pct_with_health_insurance=(
                (1 - (int(d[26]) + int(d[27]) + int(d[28]) + int(d[29])) / health) * 100
                if health is not None
                else None
            ),
            pct_foreign_born=int(d[31]) / nativity * 100
            if nativity is not None
            else None,
            pct_with_computer=int(d[33]) / computer * 100
            if computer is not None
            else None,
            pct_with_internet=(1 - int(d[35]) / internet) * 100
            if internet is not None
            else None,
            pct_no_vehicle_available=int(d[37]) / vehicle * 100
            if vehicle is not None
            else None,
        )
        print(f"Census data for coordinates ({latitude}, {longitude}): {ret}")
        return ret
    except Exception as e:
        print(
            f"Error fetching census data for coordinates ({latitude}, {longitude}): {e}"
        )
        return None


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
        with open(str(OUT_DIR) + "/mbta_gtfs_stops.csv", "r") as f:
            stop_locations = pd.read_csv(f)
        stop_locations[["station", "stop_lat", "stop_lon"]]
        stop_locations = stop_locations.rename(
            columns={"stop_lat": "latitude", "stop_lon": "longitude"}
        )
        average_boardings_by_day = average_boardings_by_day.merge(
            stop_locations[["station", "latitude", "longitude"]],
            on="station",
            how="left",
        )

        average_boardings_by_day = (
            average_boardings_by_day.groupby("station")
            .agg(
                avg_boardings_per_day=("avg_boardings_per_day", "first"),
                latitude=("latitude", "first"),
                longitude=("longitude", "first"),
            )
            .reset_index()
        )
        average_boardings_by_day["agency"] = "MBTA"
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

            with open(str(OUT_DIR) + "/uta_gtfs_stops.csv", "r") as f:
                stop_locations = pd.read_csv(f)
            stop_locations[["station", "stop_lat", "stop_lon"]]
            stop_locations = stop_locations.rename(
                columns={"stop_lat": "latitude", "stop_lon": "longitude"}
            )
            average_boardings_by_day = average_boardings_by_day.merge(
                stop_locations[["station", "latitude", "longitude"]],
                on="station",
                how="left",
            )
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
    with open(str(OUT_DIR) + "/cta_gtfs_stops.csv", "r") as f:
        stop_locations = pd.read_csv(f)
    stop_locations[["station", "stop_lat", "stop_lon"]]
    stop_locations = stop_locations.rename(
        columns={"stop_lat": "latitude", "stop_lon": "longitude"}
    )
    df = df.merge(
        stop_locations[["station", "latitude", "longitude"]],
        on="station",
        how="left",
    )
    # TODO: this is slightly wrong b/c the stations in the gtfs file don't always match the api station names :/
    df = (
        df.groupby("station")
        .agg(
            avg_boardings_per_day=("avg_boardings_per_day", "first"),
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
        )
        .reset_index()
    )
    df = df.dropna(subset=["latitude", "longitude"])

    df["agency"] = "Chicago CTA"
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

    for df in all_dfs:
        df["census_data"] = df.apply(
            lambda row: get_census_data_from_coordinates(
                row["latitude"], row["longitude"]
            ),
            axis=1,
        )
        df["population"] = df["census_data"].apply(
            lambda x: x.population if x else None
        )
        df["median_income"] = df["census_data"].apply(
            lambda x: x.median_income if x else None
        )
        df["percent_under_5"] = df["census_data"].apply(
            lambda x: x.percent_under_5 if x else None
        )
        df["pct_over_85"] = df["census_data"].apply(
            lambda x: x.pct_over_85 if x else None
        )
        df["median_household_income"] = df["census_data"].apply(
            lambda x: x.median_household_income if x else None
        )
        df["per_capita_income"] = df["census_data"].apply(
            lambda x: x.per_capita_income if x else None
        )
        df["gini_index"] = df["census_data"].apply(
            lambda x: x.gini_index if x else None
        )
        df["median_home_value"] = df["census_data"].apply(
            lambda x: x.median_home_value if x else None
        )
        df["median_rent"] = df["census_data"].apply(
            lambda x: x.median_rent if x else None
        )
        df["pct_bachelors_or_higher"] = df["census_data"].apply(
            lambda x: x.pct_bachelors_or_higher if x else None
        )
        df["pct_labor_force_unemployed"] = df["census_data"].apply(
            lambda x: x.pct_labor_force_unemployed if x else None
        )
        df["pct_married"] = df["census_data"].apply(
            lambda x: x.pct_married if x else None
        )
        df["pct_with_health_insurance"] = df["census_data"].apply(
            lambda x: x.pct_with_health_insurance if x else None
        )
        df["pct_foreign_born"] = df["census_data"].apply(
            lambda x: x.pct_foreign_born if x else None
        )
        df["pct_renter_occupied"] = df["census_data"].apply(
            lambda x: x.pct_renter_occupied if x else None
        )
        df["pct_with_computer"] = df["census_data"].apply(
            lambda x: x.pct_with_computer if x else None
        )
        df["pct_with_internet"] = df["census_data"].apply(
            lambda x: x.pct_with_internet if x else None
        )
        df["pct_no_vehicle_available"] = df["census_data"].apply(
            lambda x: x.pct_no_vehicle_available if x else None
        )
        df.drop(columns=["census_data"], inplace=True)

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.to_csv(OUT_FILE, index=False)


if __name__ == "__main__":
    main()
