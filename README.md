# Medical Data Warehouse & Telegram Scraper

**A comprehensive data pipeline for scraping, storing, and analyzing medical shipping data from Telegram channels.**

This project scrapes data from specific Telegram channels (CheMed, Lobelia Cosmetics, Tikvah Pharma), loads it into a PostgreSQL database, and transforms the data using **dbt** (data build tool) for analytics. It also includes the structure for an API to expose the processed data.

## Project Structure

```
├── api/                  # API source code
├── data/                 # Raw data storage (local)
│   ├── raw/
│       ├── images/       # Scraped images
│       └── telegram_messages/ # Scraped JSON messages
├── medical_warehouse/    # dbt project for data transformation
│   ├── models/           # dbt models (staging, marts)
│   └── ...
├── notebooks/            # Jupyter notebooks for analysis/EDA
├── scripts/              # Helper scripts
├── src/                  # Source code for ETL
│   ├── scraper.py        # Telegram scraper script
│   └── load_raw_to_postgres.py # Data loader script
├── tests/                # Tests
├── docker-compose.yml    # Docker services config
├── Dockerfile            # Docker build instructions
└── requirements.txt      # Python dependencies
```

## Prerequisites

- **Python 3.8+**
- **Docker** & **Docker Compose**
- **PostgreSQL** (if running locally without Docker)
- **Telegram API Credentials** (API ID and Hash)

## Setup & Installation

### 1. Clone the repository
```bash
git clone <repository_url>
cd shipping_data
```

### 2. Set up Environment Variables
Create a `.env` file in the root directory with the following credentials:
```env
# Telegram API
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=medical_warehouse
DB_USER=postgres
DB_PASSWORD=postgres
```

### 3. Install Python Dependencies
It is recommended to use a virtual environment.
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 🏃 Usage

### Step 1: Data Scraping
Run the scraper to fetch messages and images from the configured Telegram channels.
```bash
python src/scraper.py
```
*Output: JSON files in `data/raw/telegram_messages/` and images in `data/raw/images/`.*

### Step 2: Load Data to Database
Load the raw JSON data into your PostgreSQL database.
```bash
python src/load_raw_to_postgres.py
```

### Step 3: Data Transformation (dbt)
The `medical_warehouse` directory contains the dbt project.

1. Navigate to the dbt directory:
   ```bash
   cd medical_warehouse
   ```

2. Run dbt models:
   ```bash
   dbt run
   ```

3. Run data tests:
   ```bash
   dbt test
   ```

The dbt project transforms raw tables into:
- **Staging Models**: Cleaned raw data (`stg_telegram_messages`)
- **Marts**: Business-ready tables (`dim_channels`, `dim_dates`, `fct_message`)

### Step 4: Object Detection (YOLO)
Enrich the data lake by running object detection on scraping images.
```bash
# Run detection script (uses YOLOv8n)
python src/yolo_detect.py

# Load detections to postgres
python src/load_yolo_to_postgres.py

# Update the detections fact table
cd medical_warehouse && dbt run --select fct_image_detections
```

### Step 5: Analytical API
Start the FastAPI server to serve analytical endpoints.
```bash
uvicorn api.main:app --reload
```
Endpoints available at `http://localhost:8000/docs`:
- `GET /api/reports/top-products`: Most frequent terms.
- `GET /api/channels/{name}/activity`: Posting trends.
- `GET /api/search/messages`: Text search.
- `GET /api/reports/visual-content`: Image analytics.

### Step 6: Pipeline Orchestration (Dagster)
Run the full pipeline (Scrape -> Load -> dbt -> YOLO) using Dagster.

1. Install Dagster dependencies (if not already installed):
   ```bash
   pip install dagster dagster-webserver
   ```
2. Launch the Dagster UI:
   ```bash
   dagster dev -f pipeline.py
   ```
3. Access the UI at `http://localhost:3000` to materialize the `daily_pipeline_job`.

## 🐳 Docker Support
You can use Docker to spin up the entire environment (Project + Database).
```bash
docker-compose up --build
```
