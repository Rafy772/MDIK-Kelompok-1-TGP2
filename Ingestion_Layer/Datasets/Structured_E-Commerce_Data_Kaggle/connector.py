# -*- coding: utf-8 -*-
"""
Created on Wed Jun 25 14:16:52 2025

@author: Pongo
"""

import csv
import json
import requests
import sys
from itertools import islice

REST_PROXY_URL = "http://localhost:8082"
TOPIC = "kaggle"
CSV_FILE = "data.csv"
BATCH_SIZE = 500    # number of rows per HTTP request

def utf8_line_reader(file_path):
    """
    Yield only those lines from the file that successfully decode as UTF-8.
    Any line with a UnicodeDecodeError is silently skipped.
    """
    with open(file_path, 'rb') as f:                # open in binary
        for raw in f:
            try:
                yield raw.decode('utf-8')           # try to decode
            except UnicodeDecodeError:
                # skip this bad line entirely
                continue

def batched(reader, batch_size):
    """
    Yield lists of dicts of length <= batch_size from the CSV reader.
    """
    while True:
        chunk = list(islice(reader, batch_size))
        if not chunk:
            return
        yield chunk

def main():
    # 1) Build a reader over only the good UTF-8 lines
    line_iter = utf8_line_reader(CSV_FILE)
    try:
        reader = csv.DictReader(line_iter)
    except csv.Error as e:
        print(f"CSV parsing error: {e}", file=sys.stderr)
        sys.exit(1)

    total = 0

    # 2) For each batch of rows...
    for batch in batched(reader, BATCH_SIZE):
        # build the payload
        records = [{"value": row} for row in batch]
        payload = {"records": records}

        # 3) POST to REST Proxy
        resp = requests.post(
            f"{REST_PROXY_URL}/topics/{TOPIC}",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            data=json.dumps(payload)
        )

        if resp.ok:
            count = len(batch)
            total += count
            print(f"✔️  Sent batch of {count} records (total so far: {total})")
        else:
            print("❌ Error sending batch:", resp.status_code, resp.text, file=sys.stderr)
            sys.exit(1)

    print(f"✅ Finished! Total records sent: {total}")

if __name__ == "__main__":
    main()
