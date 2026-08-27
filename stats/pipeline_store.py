import os
import sys

import httpx

TIMEOUT = 10

if len(sys.argv) > 1:
    query = f'{sys.argv[1]} AND metadata.subjects.subject:"Nipoppy"'
else:
    query = 'metadata.subjects.subject:"Nipoppy"'

if access_token := os.getenv("ZENODO_TOKEN"):
    size = 100
    headers = {"Authorization": f"Bearer {access_token}"}
else:
    size = 25
    headers = {}

if community := os.getenv("ZENODO_COMMUNITY"):
    print("Using Zenodo community:", community)
    url = f"https://zenodo.org/api/communities/{community}/records"
else:
    url = "https://zenodo.org/api/records"

print(f'Using Zenodo query string: "{query}"')
response = httpx.get(
    url=url,
    headers=headers,
    params={
        "q": query,
        "size": size,
    },
    timeout=TIMEOUT,
)

records = response.json()["hits"]["hits"]
n_downloads = sum([record["stats"]["downloads"] for record in records])
creators = len(
    {
        creator["name"]
        for record in records
        for creator in record["metadata"]["creators"]
    }
)
print(f"{len(records)} records found")
print(f"{n_downloads} total downloads")
print(f"{creators} unique creators")
