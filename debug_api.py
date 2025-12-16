from src.api_client import GUSApiClient

client = GUSApiClient()

print("=== Debug variable_name z API ===\n")

dataset = client.fetch_p3961_data(years=[2024], unit_level=2)

print(f"Pobrano rekordów: {len(dataset.data)}\n")

variable_names = set()
for record in dataset.data:
    vn = record.get("variable_name", "")
    variable_names.add(vn)

print(f"Unikalne variable_name ({len(variable_names)}):\n")
for vn in sorted(variable_names):
    print(f'"{vn}"')

print("\n\nPrzykładowy rekord:")
if dataset.data:
    import json
    print(json.dumps(dataset.data[0], indent=2, ensure_ascii=False, default=str))