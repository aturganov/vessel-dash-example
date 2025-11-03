#!/usr/bin/env python
"""
Database schema design for ship tracking data
This script defines the database schema and creates the necessary tables.
"""

import sqlite3
import os
from datetime import datetime

# Define the database file path
DB_FILE = "test_ship_database.db"

# Create the database if it doesn't exist
def create_database():
    """Create the database if it doesn't exist"""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    conn = sqlite3.connect(DB_FILE)
    return conn

def create_schema(conn):
    """Create the database schema"""
    cursor = conn.cursor()
    
    # Create Ships table
    cursor.execute("""
    CREATE TABLE Ships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create ShipPositions table
    cursor.execute("""
    CREATE TABLE ShipPositions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ship_id INTEGER NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        speed REAL,
        direction REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ship_id) REFERENCES Ships(id)
    )
    """)
    
    # Create EnvironmentalConditions table
    cursor.execute("""
    CREATE TABLE EnvironmentalConditions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ship_position_id INTEGER NOT NULL,
        wind_direction REAL,
        wind_speed REAL,
        air_temperature REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ship_position_id) REFERENCES ShipPositions(id)
    )
    """)
    
    # Create TankData table
    cursor.execute("""
    CREATE TABLE TankData (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ship_position_id INTEGER NOT NULL,
        tank_number INTEGER NOT NULL,
        volume REAL NOT NULL,
        max_volume REAL NOT NULL,
        percentage REAL NOT NULL,
        vapor_pressure REAL,
        vapor_temperature REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ship_position_id) REFERENCES ShipPositions(id)
    )
    """)
    
    # Create indexes for better query performance
    cursor.execute("CREATE INDEX idx_ship_positions_ship_id ON ShipPositions(ship_id)")
    cursor.execute("CREATE INDEX idx_ship_positions_timestamp ON ShipPositions(timestamp)")
    cursor.execute("CREATE INDEX idx_tank_data_ship_position_id ON TankData(ship_position_id)")
    cursor.execute("CREATE INDEX idx_tank_data_tank_number ON TankData(tank_number)")
    cursor.execute("CREATE INDEX idx_env_conditions_ship_position_id ON EnvironmentalConditions(ship_position_id)")
    
    conn.commit()

def create_test_data(conn):
    """Create sample test data for the database"""
    cursor = conn.cursor()
    
    # Insert test ships
    cursor.execute("INSERT INTO Ships (name) VALUES (?)", ("Name1",))
    ship1_id = cursor.lastrowid
    
    # Generate sample position data
    base_lat = 70.0
    base_lon = 56.0
    
    for i in range(10):  # 10 time points
        timestamp = datetime(2022, 1, 1, 0, 0) + timedelta(minutes=30*i)
        lat = base_lat + i * 0.1
        lon = base_lon + i * 0.1
        speed = 25 + i * 2
        direction = 15 * i
        
        # Insert ship position
        cursor.execute("""
        INSERT INTO ShipPositions (ship_id, timestamp, latitude, longitude, speed, direction)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (ship1_id, timestamp, lat, lon, speed, direction))
        
        ship_position_id = cursor.lastrowid
        
        # Insert environmental conditions
        cursor.execute("""
        INSERT INTO EnvironmentalConditions (ship_position_id, wind_direction, wind_speed, air_temperature)
        VALUES (?, ?, ?, ?)
        """, (ship_position_id, direction + 10, 20 + i * 1.5, -10 - i * 0.5))
        
        # Insert tank data for each tank
        for tank_num in range(5):
            volume = 1000 + tank_num * 100 + i * 50
            max_volume = 50000
            percentage = volume / max_volume * 100
            vapor_pressure = 112.0 + tank_num * 2 - i * 0.5
            vapor_temperature = -100 - tank_num * 5 + i * 2
            
            cursor.execute("""
            INSERT INTO TankData (ship_position_id, tank_number, volume, max_volume, 
                                percentage, vapor_pressure, vapor_temperature)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ship_position_id, tank_num, volume, max_volume, percentage, 
                  vapor_pressure, vapor_temperature))
    
    conn.commit()
    print("Test data created successfully.")

def main():
    """Main function to create the database and test data"""
    conn = create_database()
    create_schema(conn)
    create_test_data(conn)
    conn.close()
    print(f"Database created at {DB_FILE}")
    print("Database setup completed successfully.")

if __name__ == "__main__":
    from datetime import timedelta
    main()