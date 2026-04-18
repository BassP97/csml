from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Optional
import censusgeocode as cg

OUT_DIR = Path(__file__).parent / "light_rail_data"
OUT_FILE = OUT_DIR / "stop_ridership.csv"


def denominator(val):
    v = int(val)
    return None if v <= 0 else v


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
    pct_commute_by_transit: Optional[float] = None
    pct_work_from_home: Optional[float] = None
    pct_multi_unit_housing: Optional[float] = None


def get_census_data_from_coordinates(
    latitude: float, longitude: float
) -> Optional[CensusData]:
    print("Fetching census data for coordinates:", latitude, longitude)
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
                "B08301_001E",  # transportation total
                "B08301_010E",  # transportation public transit
                "B08301_021E",  # transportation worked from home
                "B25024_001E",  # units in structure total )density)
                "B25024_004E",  # units in structure: 2
                "B25024_005E",  # units in structure: 3, 4
                "B25024_006E",  # units in structure: 5 - 9
                "B25024_007E",  # units in structure: 10 - 19
                "B25024_008E",  # units in structure: 20 - 49
                "B25024_009E",  # units in structure: 50 +
            ]
        ),
        "for": f"tract:{tract_info['TRACT']}",
        "in": f"state:{tract_info['STATE']} county:{tract_info['COUNTY']}",
        "key": "a4283f069026c52c0b81529250807a43cf87f80d",
    }
    response = requests.get(url, params=params)
    print(response.json())
    data = response.json()
    if len(data) < 2:
        print(f"No census data found for coordinates ({latitude}, {longitude})")
        return None
    d = data[1]

    pop = denominator(d[0])
    housing = denominator(d[11])
    poverty = denominator(d[13])
    education = denominator(d[15])
    labor = denominator(d[20])
    marital = denominator(d[22])
    health = denominator(d[25])
    nativity = denominator(d[30])
    computer = denominator(d[32])
    internet = denominator(d[34])
    vehicle = denominator(d[36])
    commute = denominator(d[38])
    multi_unit_total = denominator(d[41])

    ret = CensusData(
        population=int(d[0]),
        median_income=int(d[6]),
        percent_under_5=(int(d[2]) + int(d[3])) / pop * 100
        if pop is not None
        else None,
        pct_over_85=(int(d[4]) + int(d[5])) / pop * 100 if pop is not None else None,
        median_household_income=int(d[6]),
        per_capita_income=int(d[7]) if d[7] else None,
        gini_index=float(d[8]) if d[8] else None,
        median_home_value=int(d[9]) if d[9] else None,
        median_rent=int(d[10]) if d[10] else None,
        pct_renter_occupied=int(d[12]) / housing * 100 if housing is not None else None,
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
        pct_foreign_born=int(d[31]) / nativity * 100 if nativity is not None else None,
        pct_with_computer=int(d[33]) / computer * 100 if computer is not None else None,
        pct_with_internet=(1 - int(d[35]) / internet) * 100
        if internet is not None
        else None,
        pct_no_vehicle_available=int(d[37]) / vehicle * 100
        if vehicle is not None
        else None,
        pct_commute_by_transit=int(d[39]) / commute * 100
        if commute is not None
        else None,
        pct_work_from_home=int(d[40]) / commute * 100 if commute is not None else None,
        pct_multi_unit_housing=(
            sum(int(d[i]) for i in range(42, 48)) / multi_unit_total * 100
            if multi_unit_total is not None
            else None
        ),
    )
    print(f"Census data for coordinates ({latitude}, {longitude}): {ret}")
    return ret


def query_arcgis_data(base_url: str, where: str = "1=1") -> Optional[pd.DataFrame]:
    r = requests.get(
        base_url + "/query",
        params={"where": where, "returnCountOnly": "true", "f": "json"},
    )
    total = r.json().get("count", 0)
    records = []
    page_size = 1000
    offset = 0
    while offset < total:
        r2 = requests.get(
            base_url + "/query",
            params={
                "where": where,
                "outFields": "*",
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

    return pd.DataFrame(records)


def fetch_mbta_green_line() -> Optional[pd.DataFrame]:
    dfs = []

    fall24 = query_arcgis_data(
        "https://services1.arcgis.com/ceiitspzDAHrdGO1/arcgis/rest/services/Fall_2024_MBTA_Rail_Ridership_by_Hour_Route_Line_and_Stop/FeatureServer/0",
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
        df = query_arcgis_data(
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
    print(dfs)
    return pd.concat(dfs, ignore_index=True) if dfs else None


def fetch_nyc_ridership_data() -> Optional[pd.DataFrame]:
    r = requests.get(
        "",
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
    print(result)
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
    print(df)
    return df


def fetch_wmata() -> Optional[pd.DataFrame]:
    df = query_arcgis_data(
        "https://gis.mwcog.org/wa/rest/services/RTDC/MetroRailData/MapServer/0"
    )

    month_cols = [
        "Jul_2018",
        "Aug_2018",
        "Sep_2018",
        "Oct_2018",
        "Nov_2018",
        "Dec_2018",
        "Jan_2019",
        "Feb_2019",
        "Mar_2019",
        "Apr_2019",
        "May_2019",
        "Jun_2019",
    ]
    print(df)

    for col in month_cols:
        df[col] = pd.to_numeric(df["MetrorailRidershipFY19." + col], errors="coerce")

    df["avg_boardings_per_day"] = df[month_cols].mean(axis=1)

    df = df.rename(columns={"Metro_Stations.Station_Name": "station"})
    df["agency"] = "WMATA"

    res = (
        df[["station", "avg_boardings_per_day", "latitude", "longitude", "agency"]]
        .dropna(subset=["latitude", "longitude", "avg_boardings_per_day"])
        .reset_index(drop=True)
    )
    print(res)
    return res


def fetch_seattle_data() -> Optional[pd.DataFrame]:
    with open(str(OUT_DIR) + "/link_ridership.csv", "r") as f:
        df = pd.read_csv(f)
    df["agency"] = "Seattle Link"
    df["dataset"] = "daily_ridership"
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
    seattle = fetch_seattle_data()
    if seattle is not None:
        print("Fetched Seattle Link data")
        all_dfs.append(seattle)
    else:
        print("Failed to fetch Seattle Link data")

    wmata = fetch_wmata()
    if wmata is not None:
        print(f"Fetched WMATA data ({len(wmata)} stations)")
        all_dfs.append(wmata)
    else:
        print("Failed to fetch WMATA data")

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
        with ThreadPoolExecutor(max_workers=50) as executor:
            results = list(
                executor.map(
                    lambda row: get_census_data_from_coordinates(row[0], row[1]),
                    zip(df["latitude"], df["longitude"]),
                )
            )
        df["census_data"] = results
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
        df["pct_commute_by_transit"] = df["census_data"].apply(
            lambda x: x.pct_commute_by_transit if x else None
        )
        df["pct_work_from_home"] = df["census_data"].apply(
            lambda x: x.pct_work_from_home if x else None
        )
        df["pct_multi_unit_housing"] = df["census_data"].apply(
            lambda x: x.pct_multi_unit_housing if x else None
        )
        df.drop(columns=["census_data"], inplace=True)

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.to_csv(OUT_FILE, index=False)


if __name__ == "__main__":
    main()
