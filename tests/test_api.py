from src.api_client import GUSApiClient
from src.validator import DataValidator

client = GUSApiClient()

print("=== Test API GUS ===\n")

print("1. Pobieranie kategorii głównych...")
subjects = client.get_subjects()
print(f"   Znaleziono {len(subjects)} kategorii")

for s in subjects[:5]:
    print(f"   - {s.get('id')}: {s.get('name')}")

print("\n2. Pobieranie zmiennych dla P3961...")
variables = client.get_variables("P3961")
print(f"   Znaleziono {len(variables)} zmiennych")

for v in variables[:5]:
    print(f"   - {v.get('id')}: {v.get('n1', '')[:50]}")

if variables:
    var_id = str(variables[0].get('id'))
    print(f"\n3. Pobieranie danych dla zmiennej {var_id} (level=2, rok=2022)...")
    data = client.get_data_by_variable(var_id, years=[2022], unit_level=2)
    print(f"   Pobrano {len(data)} rekordów")
    
    if data:
        print("\n   Przykładowe rekordy:")
        for d in data[:3]:
            print(f"   - {d}")

print("\n4. Pobieranie danych P3961 (level=2, lata 2022-2024)...")
dataset = client.fetch_p3961_data(years=[2022, 2024], unit_level=2)
print(f"   Pobrano {len(dataset.data)} rekordów")
print(f"   Hash: {dataset.data_hash[:16]}...")
print(f"   Metadata: {dataset.metadata}")