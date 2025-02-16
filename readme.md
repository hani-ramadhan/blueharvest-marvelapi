# Marvel Characters ETL Pipeline

This project implements an ETL pipeline to collect and analyze Marvel character data, with a Streamlit dashboard for visualization.

## Prerequisites

- Docker and Docker Compose installed
- Git installed
- Marvel API keys (obtain from [Marvel Developer Portal](https://developer.marvel.com/))

## Installation

2. Run the initialization script
```bash
chmod +x init.sh
./init.sh
```

3. Set up environment variables
```bash
cp .env.example .env
```

Edit `.env` and add your Marvel API keys:
```
MARVEL_PUBLIC_KEY=your_public_key_here
MARVEL_PRIVATE_KEY=your_private_key_here
```

4. Start the Docker containers
```bash
docker compose up -d
```

5. Monitor the container status
```bash
docker compose ps
```

Wait until all services are healthy and running.

## Accessing the Services

### Airflow Dashboard

1. Access Airflow at `http://localhost:8080`

2. Login credentials:
   - Username: `peterparker`
   - Password: `spiderman`

3. Trigger the Marvel characters ETL pipeline from the DAGs view

### Streamlit Dashboard

1. Access the visualization dashboard at `http://localhost:8501`

2. The dashboard will show:
   - Character statistics
   - Comic appearance analysis
   - Interactive visualizations
   - Search functionality

Note: The Streamlit dashboard will display a waiting message until the Airflow ETL pipeline has completed its first run.

## Troubleshooting

If you encounter any issues:

1. Check container logs:
```bash
docker compose logs -f
```

2. For specific service logs:
```bash
docker compose logs -f airflow-webserver
docker compose logs -f streamlit
```

3. If services are unhealthy:
```bash
docker compose down
docker compose up -d
```

## Project Structure

```
marvel-etl/
├── dags/                 # Airflow DAG files
├── data/                 # Data storage
├── logs/                 # Airflow logs
├── streamlit/           # Streamlit dashboard
├── docker-compose.yml   # Docker configuration
└── .env                 # Environment variables
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.