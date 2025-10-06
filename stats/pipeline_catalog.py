import sys
import httpx


TIMEOUT = 10
SIZE = 9999

if len(sys.argv) > 1:
    query = f'{sys.argv[1]} AND metadata.subjects.subject:"Nipoppy"'
else:
    query = 'metadata.subjects.subject:"Nipoppy"'

print(f'Using Zenodo query string: "{query}"')
response = httpx.get(
    "https://zenodo.org/api/records",
    params={
        "q": query,
        "size": SIZE,
    },
    timeout=TIMEOUT,
)

records = response.json()["hits"]["hits"]
n_downloads = sum([record["stats"]["downloads"] for record in records])
print(f"Total downloads for query '{query}': {n_downloads}")
