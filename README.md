# Projekt WPBD - System Strumieniowego Przetwarzania Danych

## 📋 Opis Projektu

Projekt WPBD to kompleksowy system do strumieniowego przetwarzania danych, który implementuje nowoczesną architekturę big data. System automatycznie synchronizuje zmiany z bazy danych PostgreSQL do Apache Kafka, a następnie przetwarza je w czasie rzeczywistym za pomocą Apache Spark i zapisuje w formacie Delta Lake w MinIO (S3).

## 🏗️ Architektura Systemu

System składa się z następujących komponentów:

### Warstwa Danych
- **PostgreSQL 16** - relacyjna baza danych z włączoną replikacją logiczną
- **MinIO** - obiektowe przechowywanie danych kompatybilne z S3

### Warstwa Strumieniowa
- **Apache Kafka (KRaft)** - broker wiadomości do przesyłania zdarzeń
- **Debezium Connect** - Change Data Capture (CDC) dla PostgreSQL
- **Kafka UI** - interfejs webowy do zarządzania Kafka

### Warstwa Przetwarzania
- **Apache Spark 3.5.1** - przetwarzanie strumieniowe danych
  - Spark Master - koordynator klastra
  - Spark Worker - węzeł wykonawczy
  - Spark Submit (orders & users) - aplikacje strumieniowe

### Usługi Pomocnicze
- **Connector Configurer** - automatyczna konfiguracja konektorów Debezium
- **Seeder** - inicjalizacja danych testowych
- **Seeder Cyclic** - ciągłe generowanie nowych danych (co 30s)

## 🚀 Co możesz zrobić?

### 1. Uruchomienie Całego Systemu

```bash
# Uruchom wszystkie usługi
docker-compose up -d

# Sprawdź status usług
docker-compose ps

# Zobacz logi wszystkich usług
docker-compose logs -f

# Zobacz logi konkretnej usługi
docker-compose logs -f kafka
docker-compose logs -f spark-submit-orders
```

### 2. Monitorowanie i Zarządzanie

#### Kafka UI
- **URL**: http://localhost:8081
- **Funkcje**: Przeglądanie tematów, wiadomości, konsumentów

#### Spark Master UI
- **URL**: http://localhost:8082
- **Funkcje**: Monitoring zadań Spark, workery, aplikacje

#### Spark Worker UI
- **URL**: http://localhost:8013
- **Funkcje**: Status wykonywanych zadań

#### MinIO Console
- **URL**: http://localhost:9001
- **Login**: minioadmin / minioadmin
- **Funkcje**: Przeglądanie bucketów i zapisanych danych

### 3. Praca z Bazą Danych

```bash
# Połącz się z PostgreSQL
docker exec -it postgres16 psql -U app -d appdb

# Przykładowe zapytania SQL
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM orders;
SELECT * FROM users LIMIT 10;
SELECT o.*, u.full_name FROM orders o JOIN users u ON o.user_id = u.id LIMIT 10;

# Dodaj nowego użytkownika (automatycznie trafi do Kafka!)
INSERT INTO users (full_name, email, created_at) 
VALUES ('Jan Kowalski', 'jan@example.com', NOW());
```

### 4. Sprawdzanie Danych w Kafka

```bash
# Lista tematów
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Podgląd wiadomości z tematu users
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic appdb.public.users \
  --from-beginning \
  --max-messages 5

# Podgląd wiadomości z tematu orders
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic appdb.public.orders \
  --from-beginning \
  --max-messages 5
```

### 5. Praca z MinIO i Danymi w Delta Lake

```bash
# Połącz się z MinIO Client
docker exec -it mc sh

# Wyświetl buckety
mc ls minio/

# Wyświetl zawartość datalake
mc ls minio/datalake/
mc ls minio/datalake/tables/
mc ls minio/datalake/tables/orders/
mc ls minio/datalake/tables/users/

# Pobierz plik Parquet lokalnie (opcjonalnie)
mc cp minio/datalake/tables/orders/part-00000-*.parquet /tmp/
```

### 6. Testowanie CDC (Change Data Capture)

```bash
# 1. Dodaj nowego użytkownika
docker exec postgres16 psql -U app -d appdb -c \
  "INSERT INTO users (full_name, email, created_at) VALUES ('Test User', 'test@test.com', NOW());"

# 2. Sprawdź w Kafka UI (http://localhost:8081)
#    - Zobacz temat: appdb.public.users
#    - Powinieneś zobaczyć nową wiadomość z operacją "c" (create)

# 3. Zaktualizuj użytkownika
docker exec postgres16 psql -U app -d appdb -c \
  "UPDATE users SET full_name = 'Test User Updated' WHERE email = 'test@test.com';"

# 4. W Kafka pojawi się wiadomość z operacją "u" (update)

# 5. Usuń użytkownika
docker exec postgres16 psql -U app -d appdb -c \
  "DELETE FROM users WHERE email = 'test@test.com';"

# 6. W Kafka pojawi się wiadomość z operacją "d" (delete)
```

### 7. Zarządzanie Konektorami Debezium

```bash
# Lista konektorów
curl http://localhost:8083/connectors

# Status konkretnego konektora
curl http://localhost:8083/connectors/postgres-source/status | jq

# Restart konektora
curl -X POST http://localhost:8083/connectors/postgres-source/restart

# Usunięcie konektora
curl -X DELETE http://localhost:8083/connectors/postgres-source
```

### 8. Zatrzymanie i Czyszczenie

```bash
# Zatrzymaj wszystkie usługi
docker-compose down

# Zatrzymaj i usuń wolumeny (UWAGA: usuwa wszystkie dane!)
docker-compose down -v

# Zatrzymaj konkretną usługę
docker-compose stop spark-submit-orders
```

### 9. Debugowanie i Rozwiązywanie Problemów

```bash
# Sprawdź logi konkretnej usługi
docker-compose logs connector_configurer
docker-compose logs seeder
docker-compose logs spark-submit-orders

# Wejdź do kontenera
docker exec -it postgres16 bash
docker exec -it kafka bash
docker exec -it spark-master bash

# Sprawdź użycie zasobów
docker stats

# Zrestartuj konkretną usługę
docker-compose restart kafka
```

### 10. Modyfikacja Konfiguracji

#### Zmiana liczby generowanych rekordów
```bash
# Edytuj docker-compose.yml
# Znajdź sekcję 'seeder' i zmień zmienną ROWS
ROWS: "500"  # zamiast domyślnych 200
```

#### Zmiana częstotliwości cyklicznego seedowania
```bash
# W docker-compose.yml, sekcja 'seeder-cyclic'
# Zmień wartość sleep (domyślnie 30 sekund)
sleep 60;  # będzie dodawać dane co minutę
```

## 📊 Przepływ Danych

```
PostgreSQL → Debezium → Kafka → Spark Streaming → Delta Lake (MinIO)
    ↓                      ↓                            ↓
  Tables              Topics: orders,              Parquet Files
  (users,             users                        in S3-compatible
  orders)                                          storage
```

1. **Seeder** tworzy tabele i wypełnia je danymi testowymi
2. **Seeder Cyclic** co 30s dodaje nowe rekordy
3. **Debezium** przechwytuje zmiany w PostgreSQL (CDC)
4. **Kafka** przechowuje zdarzenia w tematach
5. **Spark** czyta strumienie z Kafka w czasie rzeczywistym
6. **Delta Lake** zapisuje dane w formacie Parquet w MinIO

## 🔧 Wymagania Systemowe

- Docker Desktop lub Docker Engine + Docker Compose
- Minimum 8GB RAM (zalecane 16GB)
- Minimum 10GB wolnego miejsca na dysku

## 📝 Struktura Plików

```
.
├── docker-compose.yml          # Definicja wszystkich usług
├── connector_configurer/       # Konfiguracja Debezium
│   ├── config.py
│   ├── pg-source.json
│   └── requirements.txt
├── seeder/                     # Generowanie danych testowych
│   ├── seed.py
│   ├── cyclic_job.py
│   └── requirements.txt
├── spark/                      # Aplikacje Spark Streaming
│   ├── read_orders.py
│   └── read_users.py
└── README.md
```

## 🐛 Rozwiązywanie Problemów

### Spark nie może się połączyć z Kafka
- Sprawdź czy Kafka jest uruchomiona: `docker-compose ps kafka`
- Zobacz logi: `docker-compose logs kafka`

### Debezium Connector nie działa
- Sprawdź status: `curl http://localhost:8083/connectors/postgres-source/status`
- Sprawdź logi: `docker-compose logs connect`

### Brak danych w MinIO
- Sprawdź czy Spark job działa: `docker-compose logs spark-submit-orders`
- Sprawdź Spark UI: http://localhost:8082

### PostgreSQL nie przyjmuje połączeń
- Sprawdź healthcheck: `docker-compose ps db`
- Sprawdź logi: `docker-compose logs db`

## 📚 Dodatkowe Zasoby

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## 🎯 Możliwe Rozszerzenia

1. **Dodanie nowych tabel** - rozszerz seeder i stwórz nowe Spark jobs
2. **Integracja z BI tools** - połącz się z Tableau/Power BI do MinIO
3. **Machine Learning** - użyj MLlib w Spark do analizy predykcyjnej
4. **Alerting** - dodaj monitoring i alerty dla anomalii w danych
5. **API Layer** - stwórz REST API do odpytywania Delta Lake
6. **Data Quality** - dodaj walidację jakości danych w Spark
7. **Partitioning** - zoptymalizuj Delta Lake przez partycjonowanie
8. **Time Travel** - wykorzystaj możliwości wersjonowania Delta Lake

## 📄 Licencja

Projekt edukacyjny - WPBD (Wyższa Projektowanie Baz Danych)
