#!/usr/bin/env python
"""
Main script to create and query the test SQL database
This script sets up the database and runs example queries to demonstrate functionality.
"""

import os
import sys
try:
    import matplotlib.pyplot as plt
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    
from datetime import datetime, timedelta

# Import our custom modules
import database_schema
import database_queries

# Setup function for the database
def setup_database():
    """Set up the database with schema and test data"""
    print("Setting up database...")
    database_schema.main()
    print("Database setup completed.")
    
# Run example queries function
def run_example_queries():
    """Run example queries to demonstrate database functionality"""
    print("\n" + "="*60)
    print("Running Example Queries")
    print("="*60)
    
    # Connect to database
    conn = database_queries.connect_to_database()
    
    # Example 1: Get all ships
    ships = database_queries.get_ships(conn)
    print("\nShips in database:")
    for ship in ships:
        print(f"  - {ship['name']} (ID: {ship['id']})")
    
    # Example 2: Get ship positions
    if ships:
        ship_id = ships[0]['id']
        positions = database_queries.get_ship_positions(conn, ship_id=ship_id)
        print(f"\nShip positions for {ships[0]['name']}:")
        for i, pos in enumerate(positions[:5]):  # Show first 5 positions
            print(f"  Position {i+1}: Lat {pos['latitude']:.4f}, Lon {pos['longitude']:.4f} at {pos['timestamp']}")
        
        # Example 3: Calculate distance traveled
        distance = database_queries.calculate_ship_distance(conn, ship_id)
        print(f"\nDistance traveled by {ships[0]['name']}: {distance:.2f} km")
        
        # Example 4: Get tank data for tank 0
        tank_data = database_queries.get_tank_data(conn, tank_number=0)
        print(f"\nTank 0 data for {ships[0]['name']} (first 5 records):")
        for i, data in enumerate(tank_data[:5]):
            print(f"  Record {i+1}: Volume {data['volume']:.2f}, Percentage {data['percentage']:.2f}%, Vapor Pressure {data['vapor_pressure']:.2f}")
        
        # Example 5: Get environmental conditions
        env_data = database_queries.get_environmental_conditions(conn)
        print(f"\nEnvironmental conditions for {ships[0]['name']} (first 3 records):")
        for i, data in enumerate(env_data[:3]):
            print(f"  Record {i+1}: Wind Dir {data['wind_direction']:.2f}, Wind Speed {data['wind_speed']:.2f}, Temp {data['air_temperature']:.2f}°C")
        
        # Example 6: Get tank trends for tank 0
        tank_trends = database_queries.get_tank_trends(conn, ship_id, 0)
        print(f"\nTank 0 volume trends for {ships[0]['name']}:")
        for i, trend in enumerate(tank_trends[:5]):
            print(f"  Trend {i+1}: Volume {trend['volume']:.2f} at {trend['timestamp']}")
    
    conn.close()

# Create visualization examples
def create_visualizations():
    """Create example visualizations of the data"""
    print("\n" + "="*60)
    print("Creating Visualizations")
    print("="*60)
    
    conn = database_queries.connect_to_database()
    
    # Get ships
    ships = database_queries.get_ships(conn)
    
    if ships:
        ship_id = ships[0]['id']
        
        # Create ship position plot
        print(f"\nCreating ship position plot for {ships[0]['name']}...")
        database_queries.create_ship_position_plot(conn, ship_id)
        
        # Create tank volume plot
        print(f"Creating tank volume plot for {ships[0]['name']}...")
        database_queries.create_tank_volume_plot(conn, ship_id)
        
        # Create a custom combined plot
        print("Creating custom combined plot...")
        positions = database_queries.get_ship_positions(conn, ship_id=ship_id)
        tank_data = database_queries.get_tank_data(conn, tank_number=0)
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Plot 1: Ship trajectory
        lats = [p['latitude'] for p in positions]
        lons = [p['longitude'] for p in positions]
        ax1.plot(lons, lats, 'b-', alpha=0.7, linewidth=2)
        ax1.scatter(lons[0], lats[0], color='green', s=100, label='Start')
        ax1.scatter(lons[-1], lats[-1], color='red', s=100, label='End')
        ax1.set_xlabel('Longitude')
        ax1.set_ylabel('Latitude')
        ax1.set_title('Ship Trajectory')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Tank volumes over time
        timestamps = [t['timestamp'] for t in tank_data]
        volumes = [t['volume'] for t in tank_data]
        ax2.plot(timestamps, volumes, 'g-', linewidth=2)
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Tank 0 Volume')
        ax2.set_title('Tank 0 Volume Over Time')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.show()
    
    conn.close()

# Main function
def main():
    """Main function to set up the database and run examples"""
    # Check if the database file already exists
    db_exists = os.path.exists(database_queries.DB_FILE)
    
    # Set up the database if it doesn't exist
    if not db_exists:
        setup_database()
    else:
        print(f"Database {database_queries.DB_FILE} already exists.")
        response = input("Do you want to recreate it? (y/n): ")
        if response.lower() == 'y':
            setup_database()
    
    # Run example queries
    run_example_queries()
    
    # Create visualizations
    response = input("\nDo you want to create visualizations? (y/n): ")
    if response.lower() == 'y':
        create_visualizations()
    
    print("\nDemo completed successfully!")

if __name__ == "__main__":
    main()