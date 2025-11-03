#!/usr/bin/env python
"""
Database query functions for ship tracking data
This script provides functions to interact with the ship tracking database.
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

# Define the database file path
DB_FILE = "test_ship_database.db"

# Connect to the database
def connect_to_database():
    """Connect to the database and return connection object"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def get_ships(conn):
    """Get all ships from the database"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Ships")
    ships = cursor.fetchall()
    return ships

def get_ship_positions(conn, ship_id=None, start_date=None, end_date=None):
    """Get ship positions from the database, optionally filtered by ship and date range"""
    cursor = conn.cursor()
    
    # Base query
    query = """
    SELECT sp.*, s.name as ship_name
    FROM ShipPositions sp
    JOIN Ships s ON sp.ship_id = s.id
    """
    
    # Add WHERE clauses based on filters
    params = []
    conditions = []
    
    if ship_id:
        conditions.append("sp.ship_id = ?")
        params.append(ship_id)
    
    if start_date:
        conditions.append("sp.timestamp >= ?")
        params.append(start_date)
    
    if end_date:
        conditions.append("sp.timestamp <= ?")
        params.append(end_date)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    # Execute query
    cursor.execute(query, params)
    positions = cursor.fetchall()
    return positions

def get_tank_data(conn, ship_position_id=None, tank_number=None):
    """Get tank data from the database, optionally filtered by position or tank number"""
    cursor = conn.cursor()
    
    # Base query
    query = """
    SELECT td.*, sp.timestamp, s.name as ship_name
    FROM TankData td
    JOIN ShipPositions sp ON td.ship_position_id = sp.id
    JOIN Ships s ON sp.ship_id = s.id
    """
    
    # Add WHERE clauses based on filters
    params = []
    conditions = []
    
    if ship_position_id:
        conditions.append("td.ship_position_id = ?")
        params.append(ship_position_id)
    
    if tank_number is not None:
        conditions.append("td.tank_number = ?")
        params.append(tank_number)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    # Execute query
    cursor.execute(query, params)
    tank_data = cursor.fetchall()
    return tank_data

def get_environmental_conditions(conn, ship_position_id=None):
    """Get environmental conditions from the database, optionally filtered by position"""
    cursor = conn.cursor()
    
    # Base query
    query = """
    SELECT ec.*, sp.timestamp, s.name as ship_name
    FROM EnvironmentalConditions ec
    JOIN ShipPositions sp ON ec.ship_position_id = sp.id
    JOIN Ships s ON sp.ship_id = s.id
    """
    
    # Add WHERE clause based on filter
    params = []
    condition = ""
    
    if ship_position_id:
        condition = " WHERE ec.ship_position_id = ?"
        params.append(ship_position_id)
    
    # Execute query
    cursor.execute(query + condition, params)
    env_conditions = cursor.fetchall()
    return env_conditions

def calculate_ship_distance(conn, ship_id):
    """Calculate the total distance traveled by a ship"""
    positions = get_ship_positions(conn, ship_id=ship_id)
    
    if len(positions) < 2:
        return 0
    
    # Sort positions by timestamp
    positions = sorted(positions, key=lambda x: x['timestamp'])
    
    total_distance = 0
    for i in range(1, len(positions)):
        prev_pos = positions[i-1]
        curr_pos = positions[i]
        
        # Calculate distance using haversine formula
        lat1, lon1 = np.radians(prev_pos['latitude']), np.radians(prev_pos['longitude'])
        lat2, lon2 = np.radians(curr_pos['latitude']), np.radians(curr_pos['longitude'])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        distance = 6371 * c  # Earth's radius in km
        
        total_distance += distance
    
    return total_distance

def get_tank_trends(conn, ship_id, tank_number):
    """Get tank volume trends for a specific tank of a ship"""
    cursor = conn.cursor()
    
    query = """
    SELECT td.tank_number, td.volume, td.percentage, td.vapor_pressure, td.vapor_temperature, 
           sp.timestamp, sp.latitude, sp.longitude
    FROM TankData td
    JOIN ShipPositions sp ON td.ship_position_id = sp.id
    WHERE sp.ship_id = ? AND td.tank_number = ?
    ORDER BY sp.timestamp
    """
    
    cursor.execute(query, (ship_id, tank_number))
    tank_trends = cursor.fetchall()
    return tank_trends

def create_ship_position_plot(conn, ship_id):
    """Create a plot of ship positions"""
    positions = get_ship_positions(conn, ship_id=ship_id)
    
    if not positions:
        print(f"No positions found for ship {ship_id}")
        return
    
    # Extract coordinates
    lats = [p['latitude'] for p in positions]
    lons = [p['longitude'] for p in positions]
    
    # Create plot
    plt.figure(figsize=(10, 6))
    plt.plot(lons, lats, 'b-', alpha=0.7, linewidth=2)
    plt.scatter(lons[0], lats[0], color='green', s=100, label='Start')
    plt.scatter(lons[-1], lats[-1], color='red', s=100, label='End')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Ship Trajectory')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

def create_tank_volume_plot(conn, ship_id):
    """Create a plot of tank volumes over time"""
    # Get all tank numbers for the ship
    cursor = conn.cursor()
    cursor.execute("""
    SELECT DISTINCT td.tank_number
    FROM TankData td
    JOIN ShipPositions sp ON td.ship_position_id = sp.id
    WHERE sp.ship_id = ?
    """, (ship_id,))
    tank_numbers = [row[0] for row in cursor.fetchall()]
    
    plt.figure(figsize=(12, 8))
    
    for tank_num in tank_numbers:
        tank_trends = get_tank_trends(conn, ship_id, tank_num)
        timestamps = [t['timestamp'] for t in tank_trends]
        volumes = [t['volume'] for t in tank_trends]
        
        plt.plot(timestamps, volumes, label=f'Tank {tank_num}', linewidth=2)
    
    plt.xlabel('Time')
    plt.ylabel('Volume')
    plt.title('Tank Volumes Over Time')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def run_query_example():
    """Run an example query to demonstrate the functionality"""
    conn = connect_to_database()
    
    # Example 1: Get all ships
    ships = get_ships(conn)
    print("Ships in database:")
    for ship in ships:
        print(f"  - {ship['name']} (ID: {ship['id']})")
    
    # Example 2: Get ship positions
    if ships:
        ship_id = ships[0]['id']
        positions = get_ship_positions(conn, ship_id=ship_id)
        print(f"\nShip positions for {ships[0]['name']}:")
        for i, pos in enumerate(positions[:5]):  # Show first 5 positions
            print(f"  Position {i+1}: {pos['latitude']}, {pos['longitude']} at {pos['timestamp']}")
        
        # Example 3: Calculate distance traveled
        distance = calculate_ship_distance(conn, ship_id)
        print(f"\nDistance traveled by {ships[0]['name']}: {distance:.2f} km")
        
        # Example 4: Create plots
        create_ship_position_plot(conn, ship_id)
        create_tank_volume_plot(conn, ship_id)
    
    conn.close()

if __name__ == "__main__":
    run_query_example()