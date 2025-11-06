# cs2-insight
CS2 Insight is a data engineering project designed to automatically collect, store and present Counter-Strike 2 skin prices.

The system periodically fetches data from an external API, processes it using Apache Airflow, and exposes results through a FastAPI web interface.

## Features
- Automated data collection - Airflow fetches fresh skin prices every 30 minutes from the API.
- Daily aggregation - Once a day, Airflow calculates the average price for each skin.
- FastAPI dashboard - View current prices directly through a clean REST API and web interface.
- Containerized architecture - Everything runs in Docker for simple setup and easy deployment.
- PostgreSQL storage - All price data is stored in a PostgreSQL database for reliability and easy querying.

## Tech Stack
- Python
- Apache Airflow
- FastApi
- PostgreSQL
- Docker / Docker Compose
