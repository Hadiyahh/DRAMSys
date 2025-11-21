#!/usr/bin/env python3
import sqlite3
import glob
import os
import csv

# Where your .tdb files are
TDB_GLOB = "/home/arifh/DRAMSys/DRAMSys_tier_*_*.tdb"

# Where to write the new CSV
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "metrics_summary_v2.csv")


def get_numeric_agg(conn, table_name):
    """
    For a given table, try to:
      - detect the last column (usually the numeric value, e.g. bandwidth, power, depth)
      - compute AVG() and MAX() on that column
    Returns (avg, max) or (None, None) if table missing or empty.
    """
    try:
        cur = conn.cursor()
        # Check table exists & see schema
        cur.execute(f"PRAGMA table_info('{table_name}');")
        cols = [row[1] for row in cur.fetchall()]
        if not cols:
            return (None, None)

        value_col = cols[-1]  # assume last column is the numeric metric
        cur.execute(f"SELECT AVG({value_col}), MAX({value_col}) FROM {table_name};")
        row = cur.fetchone()
        if row is None:
            return (None, None)
        return row[0], row[1]
    except sqlite3.Error:
        return (None, None)


def get_transaction_count(conn):
    """
    Return total number of rows in Transactions table, or None if missing.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM Transactions;")
        row = cur.fetchone()
        return row[0] if row is not None else None
    except sqlite3.Error:
        return None


def parse_filename(path):
    """
    Parse info from filenames like:
      DRAMSys_tier_lpddr4_id4386_ai_cpu_trace_example_ch0.tdb

    Returns: (tier, config_id, sim_id, channel)
    """
    base = os.path.basename(path)
    if not base.startswith("DRAMSys_") or not base.endswith(".tdb"):
        return None, None, None, None

    core = base[len("DRAMSys_"):-len(".tdb")]
    # core: tier_lpddr4_id4386_ai_cpu_trace_example_ch0
    parts = core.split("_")

    # Tier: first two parts, e.g. "tier_lpddr4"
    if len(parts) < 3:
        return None, None, None, None

    tier = parts[0] + "_" + parts[1]  # e.g. tier_lpddr4

    # Find idXXXX part
    config_id = None
    for p in parts:
        if p.startswith("id"):
            try:
                config_id = int(p[2:])
            except ValueError:
                config_id = None
            break

    # sim_id = everything except DRAMSys_ prefix and .tdb suffix
    sim_id = core

    # Channel: last part contains "chX" usually
    channel = None
    last = parts[-1]
    if last.startswith("ch"):
        try:
            channel = int(last[2:])
        except ValueError:
            channel = None

    return tier, config_id, sim_id, channel


def main():
    tdb_files = sorted(glob.glob(TDB_GLOB))
    if not tdb_files:
        print("No .tdb files found under /home/arifh/DRAMSys/")
        return

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tier",
            "config_id",
            "sim_id",
            "tdb_path",
            "channel",
            "avg_bw",
            "max_bw",
            "avg_power",
            "max_power",
            "avg_buf_depth",
            "max_buf_depth",
            "total_transactions",
        ])

        for path in tdb_files:
            size = os.path.getsize(path)
            # Skip 0-byte DBs
            if size == 0:
                print(f"Skipping empty DB: {path}")
                continue

            tier, config_id, sim_id, channel = parse_filename(path)
            if tier is None:
                print(f"Skipping unrecognized filename: {path}")
                continue

            try:
                conn = sqlite3.connect(path)
            except sqlite3.Error as e:
                print(f"ERROR opening {path}: {e}")
                continue

            avg_bw, max_bw = get_numeric_agg(conn, "Bandwidth")
            avg_power, max_power = get_numeric_agg(conn, "Power")
            avg_buf, max_buf = get_numeric_agg(conn, "BufferDepth")
            total_txn = get_transaction_count(conn)

            conn.close()

            writer.writerow([
                tier,
                config_id,
                sim_id,
                path,
                channel,
                avg_bw,
                max_bw,
                avg_power,
                max_power,
                avg_buf,
                max_buf,
                total_txn,
            ])

    print(f"Wrote metrics to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
