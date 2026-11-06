"""Generate the synthetic CMMS dataset MaintBot v2 queries.

The tables are deliberately "raw": cryptic status/failure codes, no column comments, and two data
quirks that are only explained in conversation (never in the data):

* status 'CNF' = closed, no fault found -> not a real failure
* line L3 logged downtime_min in *seconds* before 2026-06-01 (MES bug)

That's what makes memory measurable: an agent without memory can read the tables but can't know
these semantics; an agent with memory learned them from earlier sessions.

Writes CSVs to setup/data/ and the expected answers the eval notebook checks to setup/data/expected.json.
"""

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(7)
OUT = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

TODAY = date(2026, 10, 25)
START = date(2025, 10, 1)
L3_FIX = date(2026, 6, 1)
P3_DECOM = date(2026, 8, 15)

PLANTS = [
    ("P1", "Spokane"),
    ("P2", "Tacoma"),
    ("P3", "Boise"),
    ("P4", "Yakima"),
]

# asset_id, desc, plant, line, class, oem, model, firmware, install
equipment = []


def add(prefix, n, plant, line, cls, oem, model, fw, start_num, desc):
    for i in range(n):
        aid = f"{prefix}-{start_num + i}"
        inst = date(2015 + random.randint(0, 9), random.randint(1, 12), random.randint(1, 28))
        equipment.append([aid, f"{desc} {aid}", plant, line, cls, oem, model, fw(aid), inst.isoformat()])


add("HP", 10, "P2", "L1", "HYD_PRESS", "Schuler", "MSD-800", lambda a: "3.2.1" if a in ("HP-204", "HP-208") else "3.3.0", 201, "Hydraulic press")
add("HU", 4, "P2", "L1", "HYD_PWR", "Bosch Rexroth", "ABPAC-60", lambda a: "1.4", 201, "Hydraulic power unit")
add("HP", 6, "P1", "L2", "HYD_PRESS", "Schuler", "MSD-600", lambda a: "3.3.0", 101, "Hydraulic press")
add("HU", 2, "P1", "L2", "HYD_PWR", "Bosch Rexroth", "ABPAC-40", lambda a: "1.4", 101, "Hydraulic power unit")
add("CV", 12, "P1", "L3", "CONVEYOR", "Hytrol", "EZLogic-190", lambda a: "PF525-7.01", 301, "Conveyor")
add("CNC", 6, "P1", "L4", "CNC", "Haas", "VF-4", lambda a: "100.21", 101, "CNC mill")
add("HP", 5, "P3", "L1", "HYD_PRESS", "Schuler", "MSD-600", lambda a: "3.1.4", 301, "Hydraulic press")
add("PK", 4, "P3", "L2", "PACKAGING", "Krones", "Variopac", lambda a: "5.2", 301, "Case packer")
add("HP", 6, "P4", "L1", "HYD_PRESS", "Aida", "NS2-1500", lambda a: "2.8", 401, "Hydraulic press")
add("CP", 3, "P4", "L2", "COMPRESSOR", "Atlas Copco", "GA-37", lambda a: "4.0", 401, "Air compressor")

ASSETS = {e[0]: e for e in equipment}

FAIL_CODES = {
    "HYD_PRESS": ["HYD-LEAK", "SEAL", "HYD-PRESS", "SENSOR", "MECH-BRG"],
    "HYD_PWR": ["HYD-LEAK", "PUMP", "SENSOR"],
    "CONVEYOR": ["ELEC-VFD", "MECH-BELT", "MECH-BRG", "SENSOR"],
    "CNC": ["SPINDLE", "SENSOR", "ELEC-MOTOR"],
    "PACKAGING": ["MECH-JAM", "SENSOR"],
    "COMPRESSOR": ["PUMP", "ELEC-MOTOR"],
}
TECHS = ["T-114", "T-207", "T-231", "T-305", "T-318", "T-402"]

work_orders = []
alarms = []
wo_n = 10000


def end_date(asset):
    return P3_DECOM if asset[2] == "P3" else TODAY


for a in equipment:
    aid, cls, plant, line = a[0], a[4], a[2], a[3]
    d = START
    stop = end_date(a)
    # preventive maintenance every ~90 days
    pm = START + timedelta(days=random.randint(0, 30))
    while pm < stop:
        wo_n += 1
        dur = random.randint(60, 240)
        work_orders.append([f"WO-{wo_n}", aid, datetime.combine(pm, datetime.min.time()) + timedelta(hours=7), None, "CLS", "PM-QTR", dur, random.choice(TECHS)])
        pm += timedelta(days=90)
    # corrective work orders (Poisson-ish)
    rate = {"HYD_PRESS": 0.045, "HYD_PWR": 0.03, "CONVEYOR": 0.06, "CNC": 0.025, "PACKAGING": 0.04, "COMPRESSOR": 0.02}[cls]
    if aid == "HP-204":
        rate *= 1.6
    while d < stop:
        if random.random() < rate:
            wo_n += 1
            opened = datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            # ~25% of corrective WOs end as "closed, no fault found"
            status = "CNF" if random.random() < 0.25 else "CLS"
            if d > TODAY - timedelta(days=3) and random.random() < 0.5:
                status = "OPN"
            code = random.choice(FAIL_CODES[cls])
            minutes = random.randint(20, 480) if status != "CNF" else random.randint(10, 60)
            logged = minutes * 60 if (line == "L3" and plant == "P1" and d < L3_FIX) else minutes
            work_orders.append([f"WO-{wo_n}", aid, opened, None, status, code, logged, random.choice(TECHS)])
        d += timedelta(days=1)

for wo in work_orders:
    opened = wo[2]
    raw = wo[6]
    real_min = raw / 60 if raw > 5000 else raw
    wo[3] = (opened + timedelta(minutes=real_min + random.randint(10, 90))) if wo[4] != "OPN" else None

# Alarms: HP-204/HP-208 (firmware 3.2.1) spew false A-417 TEMP_HIGH without A-418
alarm_codes = {"A-417": "TEMP_HIGH", "A-418": "OIL_TEMP_CONFIRM", "A-220": "PRESS_LOW", "F-0021": "VFD_ENCODER_LOSS", "A-510": "SPINDLE_LOAD"}
for a in equipment:
    aid, cls = a[0], a[4]
    d = START
    while d < end_date(a):
        if aid in ("HP-204", "HP-208") and random.random() < 0.35:
            alarms.append([datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(0, 23)), aid, "A-417", round(random.uniform(61, 64), 1)])
        if cls == "HYD_PRESS" and random.random() < 0.01:
            ts = datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(0, 23))
            alarms.append([ts, aid, "A-417", round(random.uniform(70, 80), 1)])
            alarms.append([ts + timedelta(minutes=2), aid, "A-418", round(random.uniform(70, 80), 1)])
        if cls == "CONVEYOR" and random.random() < 0.02:
            alarms.append([datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(0, 23)), aid, "F-0021", 0])
        if cls == "CNC" and random.random() < 0.02:
            alarms.append([datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(0, 23)), aid, "A-510", round(random.uniform(90, 120), 1)])
        d += timedelta(days=1)

vendors = [
    ["V-01", "Parker Hannifin (direct)", "National", 49],
    ["V-07", "Midwest Hydraulics", "Pacific NW", 2],
    ["V-12", "Rockwell Automation (direct)", "National", 21],
    ["V-15", "Northwest Electric Supply", "Pacific NW", 2],
    ["V-20", "Grainger", "National", 5],
]
parts = [
    ["P-77410", "Cylinder seal kit, Buna-N, MSD-800", "HYD_PRESS", "V-01", 49, 412.0],
    ["P-77412", "Cylinder seal kit, Viton N674-70, MSD-800", "HYD_PRESS", "V-01", 49, 655.0],
    ["P-77450", "Cylinder seal kit, Buna-N, MSD-600", "HYD_PRESS", "V-01", 49, 380.0],
    ["P-81002", "Hydraulic filter element 10um", "HYD_PWR", "V-01", 14, 88.0],
    ["P-90210", "PowerFlex 525 VFD 5HP", "CONVEYOR", "V-12", 21, 1240.0],
    ["P-90233", "Encoder cable 10m M23", "CONVEYOR", "V-12", 21, 96.0],
    ["P-60011", "Conveyor drive belt EZ-190", "CONVEYOR", "V-20", 5, 145.0],
    ["P-40500", "Spindle bearing set VF-4", "CNC", "V-20", 10, 980.0],
]

with open(OUT / "equipment.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["asset_id", "asset_desc", "plant_code", "line_code", "asset_class", "oem", "model_no", "firmware_ver", "install_dt"])
    w.writerows(equipment)
with open(OUT / "plants.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["plant_code", "plant_name"])
    w.writerows(PLANTS)
with open(OUT / "work_orders.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["wo_id", "asset_id", "opened_ts", "closed_ts", "status", "fail_cd", "downtime_min", "tech_id"])
    for wo in work_orders:
        w.writerow([wo[0], wo[1], wo[2].isoformat(sep=" "), wo[3].isoformat(sep=" ") if wo[3] else "", wo[4], wo[5], wo[6], wo[7]])
with open(OUT / "alarms.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["alarm_ts", "asset_id", "alarm_cd", "reading"])
    for al in sorted(alarms):
        w.writerow([al[0].isoformat(sep=" "), al[1], al[2], al[3]])
with open(OUT / "vendors.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["vendor_id", "vendor_name", "region", "list_lead_days"])
    w.writerows(vendors)
with open(OUT / "parts_catalog.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["part_no", "part_desc", "asset_class", "default_vendor_id", "list_lead_days", "unit_cost_usd"])
    w.writerows(parts)


# ---------- ground truth for the eval rubrics ----------
def qualifying(wo):
    """A real failure: closed, not 'no fault found', not preventive."""
    return wo[4] == "CLS" and not wo[5].startswith("PM")


def in_range(ts, lo, hi):
    return lo <= ts.date() <= hi


def real_minutes(wo):
    a = ASSETS[wo[1]]
    if a[2] == "P1" and a[3] == "L3" and wo[2].date() < L3_FIX:
        return wo[6] / 60
    return wo[6]


Q3 = (date(2026, 7, 1), date(2026, 9, 30))
gold = {}
hp204_q3 = [w for w in work_orders if w[1] == "HP-204" and in_range(w[2], *Q3)]
gold["hp204_q3_real_failures"] = sum(qualifying(w) for w in hp204_q3)
gold["hp204_q3_naive_count"] = sum(not w[5].startswith("PM") for w in hp204_q3)

last_week = (TODAY - timedelta(days=28), TODAY - timedelta(days=1))
weekly = [w for w in work_orders if ASSETS[w[1]][2] == "P2" and ASSETS[w[1]][4] == "HYD_PRESS" and in_range(w[2], *last_week) and w[4] != "CNF" and not w[5].startswith("PM")]
by_line = {}
codes = {}
for w in weekly:
    by_line[ASSETS[w[1]][3]] = by_line.get(ASSETS[w[1]][3], 0) + w[6]
    codes[w[5]] = codes.get(w[5], 0) + 1
gold["maria_weekly_report"] = {"window": [str(x) for x in last_week], "downtime_min_by_line": by_line, "top_fail_codes": sorted(codes.items(), key=lambda kv: -kv[1])[:3], "wo_count": len(weekly)}

may = (date(2026, 5, 1), date(2026, 5, 31))
l3_may = [w for w in work_orders if ASSETS[w[1]][2] == "P1" and ASSETS[w[1]][3] == "L3" and in_range(w[2], *may)]
gold["l3_may_2026_downtime_hours_correct"] = round(sum(real_minutes(w) for w in l3_may) / 60, 1)
gold["l3_may_2026_downtime_hours_naive"] = round(sum(w[6] for w in l3_may) / 60, 1)
monthly = {}
for w in work_orders:
    a = ASSETS[w[1]]
    if a[2] == "P1" and a[3] == "L3" and w[2].date() >= date(2026, 1, 1):
        k = w[2].strftime("%Y-%m")
        monthly[k] = monthly.get(k, 0) + real_minutes(w)
gold["l3_monthly_downtime_min_correct"] = {k: round(v) for k, v in sorted(monthly.items())}
mar = (date(2026, 3, 1), date(2026, 3, 31))
sep = (date(2026, 9, 1), date(2026, 9, 30))
gold["l3_apr_vs_jul_2026"] = {"apr_min": gold["l3_monthly_downtime_min_correct"]["2026-04"], "jul_min": gold["l3_monthly_downtime_min_correct"]["2026-07"], "apr_naive_min": round(sum(w[6] for w in work_orders if ASSETS[w[1]][3] == "L3" and ASSETS[w[1]][2] == "P1" and w[2].strftime("%Y-%m") == "2026-04"))}

# Sofia's MTBF: calendar hours x active presses (excl. P3) / qualifying failures, Q3 2026
presses = [e for e in equipment if e[4] == "HYD_PRESS" and e[2] != "P3"]
fails = [w for w in work_orders if ASSETS[w[1]][4] == "HYD_PRESS" and ASSETS[w[1]][2] != "P3" and in_range(w[2], *Q3) and qualifying(w)]
hours = (Q3[1] - Q3[0]).days * 24 + 24
gold["mtbf_q3_presses_hours"] = round(hours * len(presses) / len(fails), 1)
gold["mtbf_q3_presses_inputs"] = {"calendar_hours": hours, "active_presses": len(presses), "qualifying_failures": len(fails)}

pareto = {}
for w in work_orders:
    a = ASSETS[w[1]]
    if a[2] != "P3" and in_range(w[2], *Q3) and qualifying(w):
        pareto.setdefault(a[2], {}).setdefault(w[5], 0)
        pareto[a[2]][w[5]] += 1
gold["pareto_q3_by_plant"] = {p: sorted(c.items(), key=lambda kv: -kv[1]) for p, c in pareto.items()}

sep_p2_press_sys = [w for w in work_orders if ASSETS[w[1]][2] == "P2" and ASSETS[w[1]][4] in ("HYD_PRESS", "HYD_PWR") and in_range(w[2], *sep)]
gold["p2_press_system_wo_sep_2026"] = len(sep_p2_press_sys)
gold["p2_press_only_wo_sep_2026"] = sum(ASSETS[w[1]][4] == "HYD_PRESS" for w in sep_p2_press_sys)
gold["p2_q3_press_real_failures"] = sum(1 for w in work_orders if ASSETS[w[1]][2] == "P2" and ASSETS[w[1]][4] == "HYD_PRESS" and in_range(w[2], *Q3) and qualifying(w))
gold["p4_asset_count"] = sum(e[2] == "P4" for e in equipment)
gold["hp210_oem"] = ASSETS["HP-210"][5]
gold["cv305_install_dt"] = ASSETS["CV-305"][8]
gold["cnc104_oct_alarm_codes"] = sorted({al[2] for al in alarms if al[1] == "CNC-104" and al[0].date() >= date(2026, 10, 1)})
gold["p1_asset_count"] = sum(e[2] == "P1" for e in equipment)

(OUT / "expected.json").write_text(json.dumps(gold, indent=2, default=str))
print(f"equipment={len(equipment)} work_orders={len(work_orders)} alarms={len(alarms)}")
