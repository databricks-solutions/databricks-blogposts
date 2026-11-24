"""Generate the scenario batches as JSON-lines files under data/batch_<n>/{legs,events}/.

Each shipment (S1..S7) exercises one case. Updates are new rows with the same key and a
larger updated_at.

  S1  single leg; newer event (b2); late EARLIER event (b3)  -> first event moves
  S2  two legs: leg A (b1), leg B + newer event (b2), late earlier event on leg B (b3)
      -> latest moves to leg B in b2; first (booking_ref) moves to leg B in b3
  S3  in-place update of the latest event (b2): same leg_id + event_time, new milestone
  S4  leg-only update (b2): consignee_account changes, no new event
  S5  leg-only update in b4 (consignee_account changes)
  S6  event arrives before its leg (b1); the leg lands in b2 (same commit as S4's update)
  S7  pure-insert commit (b3)
  b4  small change batch: S3 newer event (event side) + S5 consignee update (leg side)

  python3 gen_batches.py
"""

import json
from pathlib import Path

OUT = Path(__file__).parent / "data"


def ts(hh, mm):
    return f"2026-10-01T{hh:02d}:{mm:02d}:00.000Z"


def upd(batch):
    return f"2026-10-0{batch}T12:00:00.000Z"


def leg(b, leg_id, shipment, booking, ship_date, account, **kw):
    row = {
        "leg_id": leg_id,
        "shipment_id": shipment,
        "payer_id": f"PAYER-{shipment}",
        "booking_ref": booking,
        "carrier_ref": f"CR-{leg_id}",
        "ship_date": ship_date,
        "consignee_account": account,
        "consignee_branch": "BR01",
        "service_level": "STANDARD",
        "shipper_name": f"Shipper {shipment}",
        "declared_value": "1000.00",
        "currency": "USD",
        "is_priority": "N",
        "updated_at": upd(b),
    }
    row.update(kw)
    return row


def ev(b, leg_id, shipment, t, milestone, location="Hub East"):
    return {
        "leg_id": leg_id,
        "event_time": t,
        "shipment_id": shipment,
        "location": location,
        "milestone": milestone,
        "status_code": "OK",
        "updated_at": upd(b),
    }


BATCHES = {
    1: {
        "legs": [
            leg(1, "L1", "S1", "BK1", "2026-10-01", "ACC1"),
            leg(1, "L2A", "S2", "BK2A", "2026-10-01", "ACC2"),
            leg(1, "L3", "S3", "BK3", "2026-10-01", "ACC3"),
            leg(1, "L4", "S4", "BK4", "2026-10-01", "ACC4-OLD"),
            leg(1, "L5", "S5", "BK5", "2026-10-01", "ACC5"),
        ],
        "events": [
            ev(1, "L1", "S1", ts(9, 0), "Picked up"),
            ev(1, "L2A", "S2", ts(9, 0), "Picked up", location="Hub A"),
            ev(1, "L2A", "S2", ts(9, 10), "In transit", location="Hub A"),
            ev(1, "L3", "S3", ts(9, 0), "Picked up"),
            ev(1, "L3", "S3", ts(9, 20), "In transit"),
            ev(1, "L4", "S4", ts(9, 0), "Picked up"),
            ev(1, "L5", "S5", ts(9, 0), "Picked up"),
            ev(1, "L6", "S6", ts(9, 0), "Picked up"),
        ],
    },
    2: {
        "legs": [
            leg(2, "L2B", "S2", "BK2B", "2026-10-02", "ACC2"),      # new leg (insert)
            leg(2, "L4", "S4", "BK4", "2026-10-01", "ACC4-NEW"),    # update
            leg(2, "L6", "S6", "BK6", "2026-10-01", "ACC6"),        # late leg (insert)
        ],
        "events": [
            ev(2, "L1", "S1", ts(10, 0), "Delivered"),
            ev(2, "L2B", "S2", ts(10, 0), "Delivered", location="Hub B"),
            ev(2, "L3", "S3", ts(9, 20), "Out for delivery"),        # in-place update
        ],
    },
    3: {
        "legs": [
            leg(3, "L7", "S7", "BK7", "2026-10-03", "ACC7"),        # pure insert commit
        ],
        "events": [
            ev(3, "L1", "S1", ts(8, 0), "Label created"),            # late earlier event
            ev(3, "L2B", "S2", ts(8, 30), "Label created", location="Hub B"),  # late, leg B
            ev(3, "L7", "S7", ts(9, 0), "Picked up"),
        ],
    },
    4: {
        "legs": [
            leg(4, "L5", "S5", "BK5", "2026-10-01", "ACC5-NEW"),    # leg-side update
        ],
        "events": [
            ev(4, "L3", "S3", ts(11, 0), "Delivered"),               # event-side newer event
        ],
    },
}


def main():
    for b, tables in BATCHES.items():
        for table, rows in tables.items():
            d = OUT / f"batch_{b}" / table
            d.mkdir(parents=True, exist_ok=True)
            with open(d / f"{table}_batch_{b}.json", "w") as f:
                for r in rows:
                    f.write(json.dumps(r) + "\n")
            print(f"batch {b} {table}: {len(rows)} rows -> {d}")


if __name__ == "__main__":
    main()
