import requests
import time
import json
import re

# --- Config ---
BASE_URL = "https://polytope.lumi.apps.dte.destination-earth.eu/api/v1"
TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ6Z2VPSi1PVkIxRXdkNk01blNEamZ0Vzd1WGpKa3hxMTdkV2FQbzd6NDIwIn0.eyJleHAiOjE3NTczMjAyMTEsImlhdCI6MTc1Njk3NDYxMSwiYXV0aF90aW1lIjoxNzU2OTc0NjEwLCJqdGkiOiJvZnJ0YWM6ZGMzYTgwMGQtMGJlZC00ZGY2LTlhZGQtZTA5NWI0N2EzM2I0IiwiaXNzIjoiaHR0cHM6Ly9hdXRoLmRlc3RpbmUuZXUvcmVhbG1zL2Rlc3AiLCJzdWIiOiI3MzdhYjU3Yi1iNzI5LTRkYjYtYWM4OC0wYmVmOGEwMmU0NGIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJwb2x5dG9wZS1hcGktcHVibGljIiwic2lkIjoiYThmMzcwM2UtZjJkMS00YWEyLWJhMDEtMzUzNjgyYTU5OWQ1IiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIkRQQURfRGlyZWN0X0FjY2VzcyIsIm9mZmxpbmVfYWNjZXNzIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsicG9seXRvcGUtYXBpLXB1YmxpYyI6eyJyb2xlcyI6WyJoaWdoIl19fSwic2NvcGUiOiJvcGVuaWQgb2ZmbGluZV9hY2Nlc3MiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJkZXN0aW5hdGlvbi1lYXJ0aC1kYXRhbGFrZSIsImFjY2Vzc19ncm91cCI6WyJEUEFEX0RpcmVjdF9BY2Nlc3MiLCJvZmZsaW5lX2FjY2VzcyJdfQ.fiIwvgwZATlqa20Fa2OPVo-zNF45ARL42zlErwAt77nVweGk7VaEz85s96NHFQIJYseAfY7vnSSPMmeskiBIG01HI8glMRDMQghQ5xbtfSD_HY-pRK8WyhYyRjYFjANOTiAVMsNH8UsRmtDSGUJgz_Rpm_Zsj_kun4QXuCM0eRdKVikvHGR4KrJlqMU0tQeCt7bWtfPg8PgHWHt73oivUa1Nzipc_KZgyMEsmklFxRbfBWBjUEyqzt_gnswp2yixKhhOmo-xHDWrrAUtnMecT2G3-WyCm6PjwdqTTcRORwZL33_VMUHPmEXxvMqNJagef0oQk1GjDZwJ3X4epk_Qsw"  # your actual token
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

# --- Step 1: Submit the request ---
payload = {
    "verb": "retrieve",
    "request": {
        "activity": "ScenarioMIP",
        "class": "d1",
        "dataset": "climate-dt",
        "date": "20200101/to/20200101",
        "experiment": "SSP3-7.0",
        "expver": "0001",
        "generation": 1,
        "levtype": "sfc",
        "model": "IFS-NEMO",
        "param": "134/165/166",
        "realization": 1,
        "resolution": "high",
        "stream": "clte",
        "time": "0000",
        "type": "fc"
    }
}

print("Submitting request...")

response = requests.post(
    f"{BASE_URL}/requests/destination-earth",
    headers=HEADERS,
    json=payload
)

if response.status_code not in [200, 202]:
    print(f"Error submitting request (status {response.status_code}):", response.text)
    exit(1)

print("Request accepted.")

# --- Step 2: Extract UUID from request list ---
print("\nFetching request UUID...")

req_list = requests.get(f"{BASE_URL}/requests", headers=HEADERS)
if req_list.status_code != 200:
    print("Failed to retrieve request list:", req_list.text)
    exit(1)

requests_data = req_list.json()
request_list = requests_data.get("message", [])

if not request_list:
    print("No requests found.")
    exit(1)

latest_req = sorted(request_list, key=lambda x: x.get('last_modified', 0), reverse=True)[0]
uuid = latest_req.get("id")

if not uuid:
    print("No UUID found in latest request.")
    exit(1)

print(f"Found request UUID: {uuid}")

# --- Step 3: Poll until data is ready ---
print("\nChecking request status...")

status_url = f"{BASE_URL}/requests/{uuid}"

while True:
    status_resp = requests.get(status_url, headers=HEADERS)
    content_type = status_resp.headers.get("Content-Type", "")

    # If we got JSON, it's likely still processing
    if "application/json" in content_type:
        status_json = status_resp.json()
        status = status_json.get("status", "unknown")
        print(f"Status: {status}")
        if status in ["completed", "failed"]:
            print("Request finished with status:", status)
            if status == "failed":
                print(json.dumps(status_json, indent=2))
                break
        time.sleep(5)
        continue

    # If we got GRIB or binary file, it's ready
    elif "application/x-grib" in content_type or "application/octet-stream" in content_type:
        print("✅ Request is completed and data is ready (GRIB format).")

        # Try to get filename from headers
        disposition = status_resp.headers.get("Content-Disposition", "")
        match = re.search(r'filename="([^"]+)"', disposition)
        filename = match.group(1) if match else "output.grib"

        with open(filename, "wb") as f:
            f.write(status_resp.content)
        print(f"📁 Data saved to {filename}")
        break

    else:
        print("⚠️ Unexpected content type:", content_type)
        break
