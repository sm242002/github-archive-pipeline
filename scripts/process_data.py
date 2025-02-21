import os
import pandas as pd
import json
import gzip
from collections import Counter

DATA_DIR = "data"
OUTPUT_CSV = "push_events_count.csv"

def process_github_data():
    push_events = Counter()
    
    for file in os.listdir(DATA_DIR):
        if file.endswith(".json.gz"):
            with gzip.open(os.path.join(DATA_DIR, file), "rt", encoding="utf-8") as f:
                for line in f:  # Read JSON lines correctly
                    try:
                        event = json.loads(line.strip())  # Handle each JSON object separately
                        if event.get("type") == "PushEvent":
                            date = file[:10]  # Extract YYYY-MM-DD from filename
                            push_events[date] += 1
                    except json.JSONDecodeError:
                        print(f"⚠️ Skipping malformed JSON in {file}")

    df = pd.DataFrame(push_events.items(), columns=["date", "push_event_count"])
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Saved push event counts to {OUTPUT_CSV}")

if __name__ == "__main__":
    process_github_data()
