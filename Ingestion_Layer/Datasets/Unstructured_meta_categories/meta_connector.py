# -*- coding: utf-8 -*-
"""
Created on Wed Jun 25 2025

@author: Pongo (modified for JSONL)
"""

import json
import requests
import sys
from itertools import islice

REST_PROXY_URL = "http://localhost:8082"
TOPIC = "meta"
JSONL_FILE = "meta_Subscription_Boxes.jsonl"
BATCH_SIZE = 500    # number of records per HTTP request

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

def jsonl_reader(file_path):
    """
    Yield only those lines from the UTF-8 reader that successfully parse as JSON.
    Any line with a JSONDecodeError is silently skipped.
    """
    for line in utf8_line_reader(file_path):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            # skip invalid JSON line
            continue

def batched(iterator, batch_size):
    """
    Yield lists of items of length <= batch_size from the iterator.
    """
    while True:
        chunk = list(islice(iterator, batch_size))
        if not chunk:
            return
        yield chunk

def main():
    record_iter = jsonl_reader(JSONL_FILE)
    total = 0

    for batch in batched(record_iter, BATCH_SIZE):
        # wrap each JSON object as a record for the proxy
        records = [{"value": obj} for obj in batch]
        payload = {"records": records}

        # POST to REST Proxy
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
