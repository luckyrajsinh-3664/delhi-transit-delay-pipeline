import glob, json

prev = None
for f in sorted(glob.glob("raw_data/*.json")):
    recs = json.load(open(f, encoding="utf-8"))
    speeds = [r.get("speed") for r in recs]
    nonzero = [s for s in speeds if s not in (None, 0, 0.0)]
    print(f"\n{f}")
    print("  records           :", len(recs))
    print("  distinct vehicles :", len({r.get("vehicle_id") for r in recs}))
    print("  speed non-zero    :", len(nonzero))
    print("  max speed         :", max((s for s in speeds if s is not None), default=None))
    print("  distinct speeds   :", len(set(speeds)))
    pos = {r.get("vehicle_id"): (r.get("latitude"), r.get("longitude")) for r in recs}
    if prev is not None:
        common = set(pos) & set(prev)
        moved = sum(1 for v in common if pos[v] != prev[v])
        print(f"  seen in previous file: {len(common)} vehicles, position changed: {moved}")
    prev = pos