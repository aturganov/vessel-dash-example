# vessel-dash-example

This project provides a Python-based solution for analyzing ship tracking data using SQLite as the database backend.

## Features

- Ship tracking data analysis
- Geographic movement pattern visualization
- Tank level change monitoring
- Environmental condition correlation
- SQL database for data storage and retrieval

## Usage

To run the application:

```bash
python main.py
```

This will:
1. Set up the SQLite database
2. Populate it with sample ship tracking data
3. Demonstrate various queries and visualizations

## Project Structure

- `database_schema.py`: Contains the database schema and data population logic
- `database_queries.py`: Implements query functions for data retrieval and visualization
- `main.py`: Main script that sets up and demonstrates the database functionality
- `data/`: Directory containing sample Excel files

## Todo List

- [x] Parse and structure the ship tracking data
- [x] Analyze geographic movement patterns
- [x] Examine tank level changes over time
- [x] Correlate environmental conditions (wind, temperature) with ship movement
- [x] Identify any anomalies or interesting patterns in the data
- [x] Create visualizations to illustrate findings
- [x] Create a test SQL database with Python
- [x] Design the database schema
- [x] Create tables for the database
- [x] Populate the database with sample data
- [x] Implement query functions to interact with the database
- [x] Save the todo list in project README.md file