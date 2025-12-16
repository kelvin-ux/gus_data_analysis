from src.validator import DataValidator

validator = DataValidator()

good_records = [
    {"kod_gus": "0000000", "nazwa": "POLSKA", "poziom": "POLSKA"},
    {"kod_gus": "0200000", "nazwa": "DOLNOŚLĄSKIE", "poziom": "WOJEWODZTWO"},
]

result = validator.validate_batch(good_records, "jednostka")
print(f"Poprawne rekordy: {result.valid_count}/{result.total_input}")

bad_records = [
    {"kod_gus": "123", "nazwa": "Test", "poziom": "POLSKA"},
    {"kod_gus": "1234567", "nazwa": "", "poziom": "POWIAT"},
    {"kod_gus": "1234567", "nazwa": "Test", "poziom": "INVALID"},
]

result = validator.validate_batch(bad_records, "jednostka")
print(f"\nBłędne rekordy: {result.error_count}/{result.total_input}")
for err in result.errors:
    print(f"  - {err.error_type.value}: {err.error_message}")
