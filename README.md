# Predictive Site Outage Pipeline

This project implements a predictive site outage pipeline using Spark and PostgreSQL. The pipeline processes real-time and historical data to predict site outages and update the database accordingly.

## Project Structure

- `main.py`: Main script to run the predictive outage pipeline.
- `base.py`: Contains the `Base` class for Spark session and database interactions.
- `pg.py`: Handles PostgreSQL database connections.
- `queries.py`: SQL queries used in the pipeline.
- `predictive_outage.py`: Contains the logic for predicting site outages.
- `func.py`: Utility functions.
- `logging_setup.py`: Logging configuration.

## Workflow

1. **Data Ingestion**: Reads data from PostgreSQL tables (`messagesrealtimemqtt_energy`, `hourly_fuel_data`, `performance`, `messagesalert`, `messagesalerthistory`).
2. **Prediction**: Applies the `predictive_outage` function to generate outage predictions.
3. **Results Handling**: Writes the results to a CSV file and PostgreSQL database.
4. **Database Operations**: Executes SQL queries to update the database with the predicted outage data.

## Usage

1. Set up your environment variables:
   ```sh
   export PREDICTIVESITEOUTAGE_TEST_MODE=true
