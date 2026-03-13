import json
from datetime import datetime, timezone
from pathlib import Path


IGNORED_DATA_FILENAMES = {"metadata.json", "latest_sem.json", "departments.json"}
HISTORY_ROOT = Path("history")
_SEMESTER_NAME_CACHE = None


def _fallback_semester_name(strm):
    strm = str(strm)
    season = strm[-1]
    year = int(f"20{strm[1:3]}")
    season_name = {
        "1": "January",
        "2": "Spring",
        "6": "Summer",
        "8": "Fall",
    }.get(season, "Semester")
    return f"{season_name} {year}"


def load_semester_names():
    global _SEMESTER_NAME_CACHE

    if _SEMESTER_NAME_CACHE is not None:
        return _SEMESTER_NAME_CACHE

    mapping = {}
    previous_semesters_path = Path("previousSemesters.json")
    if previous_semesters_path.exists():
        semester_rows = json.loads(previous_semesters_path.read_text())
        mapping = {str(row["strm"]): row["name"] for row in semester_rows}

    _SEMESTER_NAME_CACHE = mapping
    return _SEMESTER_NAME_CACHE


def semester_name_for_strm(strm):
    strm = str(strm)
    return load_semester_names().get(strm, _fallback_semester_name(strm))


def normalize_timestamp(timestamp_value):
    if isinstance(timestamp_value, (int, float)):
        dt = datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
    else:
        timestamp_text = str(timestamp_value).strip()
        if not timestamp_text:
            dt = datetime.now(timezone.utc)
        else:
            dt = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

    return dt.isoformat().replace("+00:00", "Z")


def normalize_integer(value):
    if value in (None, "", "-"):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def normalize_instructors(instructors):
    if not isinstance(instructors, list):
        return []

    normalized = []
    for instructor in instructors:
        if not isinstance(instructor, dict):
            continue

        normalized.append(
            {
                "name": str(instructor.get("name", "")).strip(),
                "email": str(instructor.get("email", "")).strip(),
            }
        )

    return normalized


def create_history_document(strm, department):
    return {
        "strm": str(strm),
        "semester": semester_name_for_strm(strm),
        "department": department,
        "updated_at": None,
        "courses": {},
    }


def load_history_document(history_path, strm, department):
    history_path = Path(history_path)
    if not history_path.exists():
        return create_history_document(strm, department)

    history_document = json.loads(history_path.read_text())
    history_document.setdefault("strm", str(strm))
    history_document.setdefault("semester", semester_name_for_strm(strm))
    history_document.setdefault("department", department)
    history_document.setdefault("updated_at", None)
    history_document.setdefault("courses", {})

    return history_document


def history_path_for_department(strm, department):
    return HISTORY_ROOT / str(strm) / f"{department}.json"


def is_catalog_data_filename(path):
    path = Path(path)
    return (
        path.suffix == ".json"
        and len(path.parts) == 3
        and path.parts[0] == "data"
        and path.name not in IGNORED_DATA_FILENAMES
    )


def iter_catalog_data_files(strm):
    data_dir = Path("data") / str(strm)
    if not data_dir.exists():
        return []

    return sorted(
        path
        for path in data_dir.glob("*.json")
        if path.name not in IGNORED_DATA_FILENAMES
    )


def course_key(course):
    return f"{course.get('subject', '')}::{course.get('catalog_number', '')}"


def build_session_key(session, session_index, seen_keys):
    class_section = str(session.get("class_section") or "").strip() or "TBA"
    section_type = str(session.get("section_type") or "").strip() or "Section"
    class_nbr = normalize_integer(session.get("class_nbr"))

    session_key = f"{section_type}::{class_section}"
    if session_key in seen_keys and class_nbr is not None:
        session_key = f"{session_key}::{class_nbr}"
    if session_key in seen_keys:
        session_key = f"{session_key}::{session_index}"

    return session_key


def _metrics_match(existing_point, new_point):
    metric_fields = ("enrollment_total", "class_capacity", "wait_tot", "wait_cap")
    return all(existing_point.get(field) == new_point.get(field) for field in metric_fields)


def _update_session_points(session_history, new_point):
    points = session_history.setdefault("points", [])
    if not points:
        points.append(new_point)
        return True

    last_point = points[-1]
    if last_point.get("timestamp") == new_point["timestamp"]:
        if last_point != new_point:
            points[-1] = new_point
            return True
        return False

    if _metrics_match(last_point, new_point):
        return False

    points.append(new_point)
    return True


def _update_field(history_entry, field_name, new_value):
    if history_entry.get(field_name) == new_value:
        return False

    history_entry[field_name] = new_value
    return True


def update_department_history(history_document, department_snapshot, snapshot_timestamp):
    normalized_timestamp = normalize_timestamp(snapshot_timestamp)
    changed = False

    courses = history_document.setdefault("courses", {})

    for course_list in department_snapshot.values():
        for course in course_list:
            key = course_key(course)
            course_history = courses.setdefault(
                key,
                {
                    "subject": str(course.get("subject", "")),
                    "subject_descr": str(course.get("subject_descr", "")),
                    "catalog_number": str(course.get("catalog_number", "")),
                    "descr": str(course.get("descr", "")),
                    "topic": course.get("topic"),
                    "units": course.get("units"),
                    "sessions": {},
                },
            )

            changed |= _update_field(course_history, "subject", str(course.get("subject", "")))
            changed |= _update_field(course_history, "subject_descr", str(course.get("subject_descr", "")))
            changed |= _update_field(course_history, "catalog_number", str(course.get("catalog_number", "")))
            changed |= _update_field(course_history, "descr", str(course.get("descr", "")))
            changed |= _update_field(course_history, "topic", course.get("topic"))
            changed |= _update_field(course_history, "units", course.get("units"))

            session_histories = course_history.setdefault("sessions", {})
            seen_keys = set()

            for session_index, session in enumerate(course.get("sessions", [])):
                session_key = build_session_key(session, session_index, seen_keys)
                seen_keys.add(session_key)

                session_history = session_histories.setdefault(
                    session_key,
                    {
                        "session_key": session_key,
                        "class_nbr": normalize_integer(session.get("class_nbr")),
                        "class_section": str(session.get("class_section", "")),
                        "section_type": str(session.get("section_type", "")),
                        "topic": session.get("topic"),
                        "units": session.get("units"),
                        "instructors": normalize_instructors(session.get("instructors")),
                        "points": [],
                    },
                )

                changed |= _update_field(session_history, "class_nbr", normalize_integer(session.get("class_nbr")))
                changed |= _update_field(session_history, "class_section", str(session.get("class_section", "")))
                changed |= _update_field(session_history, "section_type", str(session.get("section_type", "")))
                changed |= _update_field(session_history, "topic", session.get("topic"))
                changed |= _update_field(session_history, "units", session.get("units"))
                changed |= _update_field(
                    session_history,
                    "instructors",
                    normalize_instructors(session.get("instructors")),
                )

                point = {
                    "timestamp": normalized_timestamp,
                    "enrollment_total": normalize_integer(session.get("enrollment_total")),
                    "class_capacity": normalize_integer(session.get("class_capacity")),
                    "wait_tot": normalize_integer(session.get("wait_tot")),
                    "wait_cap": normalize_integer(session.get("wait_cap")),
                }
                changed |= _update_session_points(session_history, point)

    if changed:
        history_document["updated_at"] = normalized_timestamp

    return changed


def _session_sort_key(session_entry):
    class_nbr = session_entry.get("class_nbr")
    class_section = str(session_entry.get("class_section") or "")
    section_type = str(session_entry.get("section_type") or "")
    return (
        section_type,
        class_section,
        class_nbr if class_nbr is not None else 10**9,
        session_entry.get("session_key", ""),
    )


def _sorted_history_document(history_document):
    sorted_courses = {}
    for key in sorted(history_document.get("courses", {})):
        course = dict(history_document["courses"][key])
        sessions = course.get("sessions", {})
        course["sessions"] = {
            session_key: session
            for session_key, session in sorted(
                sessions.items(),
                key=lambda item: _session_sort_key(item[1]),
            )
        }
        sorted_courses[key] = course

    return {
        "strm": history_document.get("strm"),
        "semester": history_document.get("semester"),
        "department": history_document.get("department"),
        "updated_at": history_document.get("updated_at"),
        "courses": sorted_courses,
    }


def write_history_document(history_document, history_path):
    history_path = Path(history_path)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(json.dumps(_sorted_history_document(history_document), separators=(",", ":")))


def metadata_timestamp_for_strm(strm):
    metadata_path = Path("data") / str(strm) / "metadata.json"
    if not metadata_path.exists():
        return normalize_timestamp(datetime.now(timezone.utc))

    metadata = json.loads(metadata_path.read_text())
    return normalize_timestamp(metadata.get("last_updated"))


def generate_history_for_strm(strm):
    snapshot_timestamp = metadata_timestamp_for_strm(strm)
    changed_departments = 0

    for data_path in iter_catalog_data_files(strm):
        department_snapshot = json.loads(data_path.read_text())
        department = data_path.stem
        history_path = history_path_for_department(strm, department)
        history_document = load_history_document(history_path, strm, department)

        if update_department_history(history_document, department_snapshot, snapshot_timestamp) or not history_path.exists():
            write_history_document(history_document, history_path)
            changed_departments += 1

    return changed_departments
