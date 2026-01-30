# Testy Jednostkowe dla Seeder

Ten katalog zawiera testy jednostkowe dla plików `seed.py` i `cyclic_job.py`.

## Struktura plików

- `test_seed.py` - testy jednostkowe dla `seed.py`
- `test_cyclic_job.py` - testy jednostkowe dla `cyclic_job.py`
- `Dockerfile.test` - Dockerfile do uruchamiania testów w kontenerze Docker
- `requirements.txt` - zależności Pythona (włączając pytest)

## Uruchomienie testów

### Używając Docker (zalecane)

Uruchom testy w kontenerze Docker używając docker-compose:

```bash
# Z głównego katalogu projektu
docker-compose -f docker-compose.test.yml up --build

# Lub bezpośrednio z katalogu seeder
cd seeder
docker build -f Dockerfile.test -t seeder-tests .
docker run --rm seeder-tests
```

### Używając pytest lokalnie

Jeśli masz zainstalowane zależności lokalnie:

```bash
cd seeder
pip install -r requirements.txt
pytest -v
```

### Uruchomienie konkretnych testów

```bash
# Test tylko dla seed.py
pytest test_seed.py -v

# Test tylko dla cyclic_job.py
pytest test_cyclic_job.py -v

# Test z pokryciem kodu
pytest --cov=. --cov-report=html --cov-report=term-missing
```

## Raport pokrycia kodu

Po uruchomieniu testów z opcją `--cov-report=html`, raport HTML zostanie wygenerowany w katalogu `htmlcov/`. Otwórz `htmlcov/index.html` w przeglądarce, aby zobaczyć szczegółowy raport pokrycia kodu.

## Zakres testów

### test_seed.py

- **TestSeedConfiguration**: Testuje konfigurację i ładowanie zmiennych środowiskowych
  - Domyślne wartości zmiennych środowiskowych
  - Niestandardowe zmienne środowiskowe
  - Format URL bazy danych

- **TestDatabaseOperations**: Testuje operacje na bazie danych
  - Tworzenie tabel (DDL)
  - Generowanie danych użytkowników
  - Generowanie danych zamówień
  - Obsługa konfliktów (ON CONFLICT)

- **TestDataValidation**: Testuje walidację i integralność danych
  - Generowanie znaczników czasu z strefą UTC
  - Zakres kwot zamówień (10-500)
  - Liczba zamówień na użytkownika (1-3)

- **TestOutputAndLogging**: Testuje wyjście i logowanie
  - Komunikaty o sukcesie

### test_cyclic_job.py

- **TestCyclicJobConfiguration**: Testuje konfigurację
  - Domyślne i niestandardowe zmienne środowiskowe
  - Format URL bazy danych

- **TestCyclicJobDatabaseOperations**: Testuje operacje na bazie danych
  - Brak tworzenia tabel (w przeciwieństwie do seed.py)
  - Generowanie danych użytkowników i zamówień
  - Obsługa konfliktów

- **TestCyclicJobDataValidation**: Testuje walidację danych
  - Generowanie znaczników czasu
  - Zakres kwot i liczby zamówień

- **TestCyclicJobOutputAndLogging**: Testuje wyjście
  - Komunikaty o sukcesie

- **TestCyclicJobUserCount**: Testuje pobieranie liczby użytkowników
  - Zapytanie o wszystkich użytkowników

## Zależności

- `pytest>=8.3.4` - Framework do testowania
- `pytest-cov>=6.0.0` - Rozszerzenie do pomiaru pokrycia kodu
- `pytest-mock>=3.14.0` - Rozszerzenie do mockowania

Wszystkie zależności są wymienione w pliku `requirements.txt`.
