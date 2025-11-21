#!/usr/bin/env python3
import csv
import sqlite3
from pathlib import Path


def resolve_paths():
    scripts_dir = Path(__file__).resolve().parent
    experiments_dir = scripts_dir  # extractor.py is directly under experiments/
    dramsys_root = experiments_dir.parent

    return {
        "experiments_dir": experiments_dir,
        "dramsys_root": dramsys_root,
        "results_root": experiments_dir / "results",
    }


def safe_single_value(cur, query):
    cur.execute(query)
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def extract_metrics_from_db(db_path: Path):
    """
    Open one DRAMSys .tdb file and pull out basic metrics.
    Returns a dict (may contain Nones if some tables are empty).
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Time / BW / power / queue depth
    avg_bw = safe_single_value(cur, "SELECT AVG(AverageBandwidth) FROM Bandwidth")
    max_bw = safe_single_value(cur, "SELECT MAX(AverageBandwidth) FROM Bandwidth")
    sim_time = safe_single_value(cur, "SELECT MAX(Time) FROM Bandwidth")

    avg_power = safe_single_value(cur, "SELECT AVG(AveragePower) FROM Power")
    max_power = safe_single_value(cur, "SELECT MAX(AveragePower) FROM Power")

    avg_buf_depth = safe_single_value(
        cur, "SELECT AVG(AverageBufferDepth) FROM BufferDepth"
    )
    max_buf_depth = safe_single_value(
        cur, "SELECT MAX(AverageBufferDepth) FROM BufferDepth"
    )

    # Transaction counts
    total_tx = safe_single_value(cur, "SELECT COUNT(*) FROM Transactions")

    # You may need to tweak the WHERE depending on actual Command strings.
    # After you run once, check DISTINCT Command values.
    try:
        reads = safe_single_value(
            cur, "SELECT COUNT(*) FROM Transactions WHERE Command LIKE 'READ%'"
        )
        writes = safe_single_value(
            cur, "SELECT COUNT(*) FROM Transactions WHERE Command LIKE 'WRITE%'"
        )
    except sqlite3.OperationalError:
        # Fallback: if commands are short like 'R', 'W', etc.
        reads = safe_single_value(
            cur, "SELECT COUNT(*) FROM Transactions WHERE Command IN ('R','READ')"
        )
        writes = safe_single_value(
            cur, "SELECT COUNT(*) FROM Transactions WHERE Command IN ('W','WRITE')"
        )

    # GeneralInfo is usually a single row
    cur.execute("SELECT * FROM GeneralInfo")
    gi = cur.fetchone()
    general_info = {}
    if gi is not None:
        # Map by column index
        col_names = [d[1] for d in cur.execute("PRAGMA table_info(GeneralInfo)")]
        general_info = dict(zip(col_names, gi))

    conn.close()

    m = {
        "avg_bw_Gbps": avg_bw,
        "max_bw_Gbps": max_bw,
        "sim_time_units": sim_time,
        "avg_power_W": avg_power,
        "max_power_W": max_power,
        "avg_buf_depth": avg_buf_depth,
        "max_buf_depth": max_buf_depth,
        "total_transactions": total_tx,
        "read_transactions": reads,
        "write_transactions": writes,
    }

    # Add a few useful GeneralInfo fields if present
    for key in ["clk", "UnitOfTime", "MCconfig", "Memspec", "Traces"]:
        if key in general_info:
            m[f"GI_{key}"] = general_info[key]

    return m


def main():
    paths = resolve_paths()
    dramsys_root = paths["dramsys_root"]
    results_root = paths["results_root"]

    # Collect all manifest CSVs recursively (one per tier folder)
    manifest_files = sorted(results_root.glob("**/*_runs_manifest.csv"))

    if not manifest_files:
        raise SystemExit(f"No *_runs_manifest.csv found under {results_root}")

    out_path = results_root / "metrics_summary.csv"
    print(f"Writing metrics to {out_path}")

    # Output header (we'll build the union of fields as we go)
    fieldnames = [
        "tier",
        "config_id",
        "sim_id",
        "trace_name",
        "simconfig",
        "channel",
    ]
    metric_fields = [
        "avg_bw_Gbps",
        "max_bw_Gbps",
        "sim_time_units",
        "avg_power_W",
        "max_power_W",
        "avg_buf_depth",
        "max_buf_depth",
        "total_transactions",
        "read_transactions",
        "write_transactions",
        "GI_clk",
        "GI_UnitOfTime",
        "GI_MCconfig",
        "GI_Memspec",
        "GI_Traces",
    ]
    fieldnames.extend(metric_fields)

    with out_path.open("w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for manifest in manifest_files:
            print(f"Processing manifest {manifest}")
            with manifest.open("r", newline="") as mf:
                reader = csv.DictReader(mf)
                for row in reader:
                    tier = row["tier"]
                    config_id = row["config_id"]
                    trace_name = row["trace_name"]
                    simconfig = row["simconfig"]

                    trace_stem = Path(trace_name).stem
                    simconfig_stub = Path(simconfig).stem
                    sim_id = f"{tier}_id{config_id}_{trace_stem}"

                    for ch in range(4):  # assume up to 4 channels; skip missing
                        db_name = f"DRAMSys_{sim_id}_{simconfig_stub}_ch{ch}.tdb"
                        db_path = dramsys_root / db_name
                        if not db_path.exists():
                            continue

                        print(f"  {sim_id} ch{ch}: {db_path}")
                        metrics = extract_metrics_from_db(db_path)

                        out_row = {
                            "tier": tier,
                            "config_id": config_id,
                            "sim_id": sim_id,
                            "trace_name": trace_name,
                            "simconfig": simconfig,
                            "channel": ch,
                        }
                        out_row.update(metrics)
                        writer.writerow(out_row)

    print("Done.")


if __name__ == "__main__":
    main()
