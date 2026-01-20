import requests
from datetime import date
from typing import List, Dict, Any

def get_line_routes(app_id: str = None, app_key: str = None):
    url = f"https://api.tfl.gov.uk/Line/Mode/bus/Route?app_id={app_id}&app_key={app_key}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def get_stops_sequence(id: str = None, app_id: str = None, app_key: str = None):
    # e.g. https://api.tfl.gov.uk/Line/12/Route/Sequence/all 
    if not id:
        raise ValueError("id must be provided")

    resp_inbound = requests.get(f"https://api.tfl.gov.uk/Line/{id}/Route/Sequence/inbound?app_id={app_id}&app_key={app_key}", timeout=10)
    resp_inbound.raise_for_status()
    resp_outbound = requests.get(f"https://api.tfl.gov.uk/Line/{id}/Route/Sequence/outbound?app_id={app_id}&app_key={app_key}", timeout=10)
    resp_outbound.raise_for_status()
    return resp_inbound.json(), resp_outbound.json()


def get_arrivals(ids=None, app_id: str = None, app_key: str = None):
    # Get the list of arrival predictions for given line ids based at the given stop
    if not ids:
        raise ValueError("ids must be provided")

    # Accept list or set
    if isinstance(ids, (list, set, tuple)):
        ids = ",".join(str(i) for i in ids)

    url = f"https://api.tfl.gov.uk/Line/{ids}/Arrivals"
    params = {}
    if app_id and app_key:
        params["app_id"] = app_id
        params["app_key"] = app_key
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

def get_timetable(
    id: str,
    stop_id: str,
    app_id: str = None,
    app_key: str = None,
    max_retries: int = 5,
    base_sleep: float = 1.0,
):
    # e.g. https://api.tfl.gov.uk/Line/12/Timetable/490006652N
    if not id:
        raise ValueError("id must be provided")
    if not stop_id:
        raise ValueError("stop_id must be provided")

    params = {}
    if app_id and app_key:
        params["app_id"] = app_id
        params["app_key"] = app_key

    url = f"https://api.tfl.gov.uk/Line/{id}/Timetable/{stop_id}"

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 429:
                sleep_time = base_sleep * (2 ** (attempt - 1))
                time.sleep(sleep_time)
                continue

            if status in (400, 404):
                # Expected for data absence or invalid stop/line
                return None

            raise
        
    raise RuntimeError(f"Exceeded retries for timetable line={id}, stop={stop_id}")

def extract_timetable_rows(
    timetable_json: Dict[str, Any],
    snapshot_date: date | None = None,
    stop_sequence: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Transform TfL timetable response into normalized rows.

    One row = one scheduled arrival per stop per line per service day.
    """
    if snapshot_date is None:
        snapshot_date = date.today()
    if timetable_json is None:
        raise ValueError("timetable_json must be provided")
    if stop_sequence is None:
        raise ValueError("stop_sequence must be provided")

    rows: List[Dict[str, Any]] = []

    line_id = timetable_json.get("lineId")
    direction = timetable_json.get("direction")
    stop_id = timetable_json.get("timetable", {}).get("departureStopId")

    routes = timetable_json.get("timetable", {}).get("routes", [])

    for route in routes:
        schedules = route.get("schedules", [])

        for schedule in schedules:
            service_day = schedule.get("name", "unknown")
            known_journeys = schedule.get("knownJourneys", [])

            for journey in known_journeys:
                hour = int(journey.get("hour"))
                minute = int(journey.get("minute"))

                rows.append({
                    "snapshot_date": snapshot_date,
                    "line_id": line_id,
                    "stop_id": stop_id,
                    "stop_sequence": stop_sequence,
                    "direction": direction,
                    "service_day": service_day,
                    "arrival_time": f"{hour:02d}:{minute:02d}",
                    "arrival_minutes": hour * 60 + minute,
                    "interval_id": journey.get("intervalId"),
                    "source": "tfl_api",
                })

    return rows