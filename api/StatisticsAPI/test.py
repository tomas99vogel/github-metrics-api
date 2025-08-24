
from datetime import datetime

timestamps = []
opened_events = ["2025-08-24T10:12:17Z", "2025-08-24T09:34:17Z"]

for event in opened_events:
    dt = datetime.fromisoformat(event.replace('Z', '+00:00'))
    print(dt)
    timestamps.append(dt.timestamp())
    
timestamps.sort()
print(timestamps)
    
time_diffs = []
for i in range(1, len(timestamps)):
    diff = timestamps[i] - timestamps[i-1]
    time_diffs.append(diff)
    
# Return average in seconds
print(sum(time_diffs) / len(time_diffs))