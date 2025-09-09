# Real-time Backend für eine datenintensive Applikation

Dieses Projekt implementiert die folgende **Lambda-Architektur** für ein Echtzeit-Reporting:

![Architektur-Konzept](/docs/architektur.png)

## Architekturüberblick

Jede Komponente stellt relevante Aufruf-Metriken für den Monitoring-Stack zur Verfügung.

### Echtzeit-API

- Ein FastAPI-Service liefert kontinuierlich Timestamp-Events.

#### Data-Ingestion-Layer

- Sekündlich wird ein Timestamp von der Echtzeit-API abgefragt und als Nachricht in ein **Kafka-Streaming-Topic**
  geschrieben.

### Speed-Layer

- Ein Kafka-Consumer liest den Stream, aggregiert die Events in **Tumbling-Windows** mit jeweils 10 Sekunden und speichert die Ergebnisse in **HBase**.

### Batch-Layer

- **Kaggle NYC Taxi-Import**: Beim Anwendungsstart wird der Kaggle-Datensatz in HBase geladen.
- **Scheduler**: Aggregiert einmalig beim Start und täglich um 0 Uhr die Vortagesdaten (aus Kafka + Kaggle-Datensatz) und schreibt die Ergebnisse in eine Aggregations-Tabelle.

### Serving-Layer

- HBase persistiert die Ergebnisse aus Batch- und Speed-Layer.
- HBase stellt die Daten für ein zukünftiges Reporting wahlweise per SQL oder API zur Verfügung.

## Monitoring & Sicherheit

- **Prometheus** sammelt Metriken aus API, Speed-, Batch und Data-Ingestion-Layern. 
- **Grafana** visualisiert Prometheus-Metriken wie Durchsatz, Latenzen und Event-Zahlen in einem Dashboard.
- Die Umsetzung ist in einem internen "lambda-net"-Netzwerk erfolgt und Grafana verfügt über eine Benutzerauthentifizierung

---

## Kurzanleitung

### 1. Repository klonen

```
git clone https://github.com/florianStolzmannStudy/DLMDWWDE02-lambda-real-time-reporting.git
cd lambda-real-time-reporting
```

### 2. Installation und Start von Docker

Installieren Sie bei Bedarf Docker und starten Sie Docker (bei Windows Docker Desktop).

### 3. Docker-Images bauen und starten

```
docker compose up --build
```

Beim ersten Start werden die Images gebaut und alle Komponenten gestartet.
Je nach System- und Netzwerkgeschwindigkeit kann dieses einige Minuten dauern.

---

## Monitoring

- **Prometheus**: [Prometheus-UI](http://localhost:9091)
  - Der Health-Status der Komponenten ist unter  [Prometheus-UI-Status](http://localhost:9091/targets) aufrufbar. 
- **Grafana**: [Grafana-UI](http://localhost:3000)  
  - Login: `admin / admin` - das Passwort muss beim ersten Login geändert werden.
  - Ein [Metriken-Dashboard](http://localhost:3000/d/e8d6b729-9137-42b6-a210-c03c67837355/lambda-real-time-reporting) ist als `dashboard.json` bereits im Projekt hinterlegt und wird automatisch bei Anwendungsstart provisioniert.

### Metriken-Dashboard:

![Dashboard Screenshot](/docs/dashboard.png)

---

## HBase-Abfragen

| Aktion                    | Befehl                              | Beschreibung                                        | Beispielausgabe                                    |
|---------------------------|-------------------------------------|-----------------------------------------------------|----------------------------------------------------|
| Verbindung herstellen     | `docker exec -it hbase hbase shell` | Startet die interaktive HBase-Shell                 | —                                                  |
| Tabellen auflisten        | `list`                              | Zeigt alle vorhandenen Tabellen                     | `TABLE taxi_agg, taxi_raw, taxi_realtime_tumbling` |
| Anzahl Einträge abfragen  | `count 'taxi_raw'`                  | Zählt alle Zeilen in der Tabelle                    | `2.964.624 Datensätze`                             |
| Ersten Datensatz abfragen | `scan 'taxi_agg', {LIMIT => 1}`     | Zeigt den ersten Datensatz der Aggregations-Tabelle | —                                                  |

---

## Relevante Endpunkte

| Service       | URL                                              | Beschreibung                                                           |
|---------------|--------------------------------------------------|------------------------------------------------------------------------|
| API-Service   | [Health-Check](http://127.0.0.1:8000/health)     | Gibt den Status des API-Service zurück                                 |
| API-Service   | [Timestamp-Stream](http://127.0.0.1:8000/stream) | Liefert einen aktuellen Zeitstempel (vom Ingestor konsumiert)          |
| API-Service   | [Metrics-API](http://127.0.0.1:8000/metrics)     | Prometheus-Metriken des API-Services, z. B. `api_requests_total`       |
| Serving-Layer | [HBase-UI](http://127.0.0.1:16010/)              | Zugriff auf die HBase-UI zum Zugriff auf die HBse-Server und -Tabellen | 

## Weiterführende Arbeiten

- Mittels YML-Konfiguration können die Kafka-Komponenten, sowie die HBase-Datenbank je nach Bedarf manuell oder automatisch skaliert werden.
- Über einen Prometheus-Alertmanager kann bei Bedarf ein Alerting auf Basis des Health-Status je Komponente implementiert werden, beispielsweise wenn eine Komponente nicht korrekt verfügbar (Status != OK) ist.
- Bei Nutzung von Grafana als Dashboard müsste noch eine noSQL-`Data source` für den Zugriff auf HBase installiert und konfiguriert werden. 
