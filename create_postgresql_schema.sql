DROP TABLE IF EXISTS f_data CASCADE;
DROP TABLE IF EXISTS d_calendar CASCADE;
DROP TABLE IF EXISTS d_ship CASCADE;

CREATE TABLE d_ship (
    ship_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE d_calendar (
    datetime_id TIMESTAMP PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, day, hour, minute),
    UNIQUE(datetime_id)
);

CREATE TABLE f_data (
    fact_key SERIAL PRIMARY KEY,
    ship_id VARCHAR(50) NOT NULL,
    datetime_id TIMESTAMP NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    wind_direction DECIMAL(5, 2),
    wind_speed DECIMAL(5, 2),
    air_temperature DECIMAL(5, 2),
    tank0_liquid_volume DECIMAL(10, 2),
    tank0_max_volume DECIMAL(10, 2),
    tank0_percentage DECIMAL(5, 2),
    tank1_liquid_volume DECIMAL(10, 2),
    tank1_max_volume DECIMAL(10, 2),
    tank1_percentage DECIMAL(5, 2),
    tank2_liquid_volume DECIMAL(10, 2),
    tank2_max_volume DECIMAL(10, 2),
    tank2_percentage DECIMAL(5, 2),
    tank3_liquid_volume DECIMAL(10, 2),
    tank3_max_volume DECIMAL(10, 2),
    tank3_percentage DECIMAL(5, 2),
    tank4_liquid_volume DECIMAL(10, 2),
    tank4_max_volume DECIMAL(10, 2),
    tank4_percentage DECIMAL(5, 2),
    tank0_vapor_pressure DECIMAL(6, 2),
    tank0_vapor_temperature DECIMAL(6, 2),
    tank1_vapor_pressure DECIMAL(6, 2),
    tank1_vapor_temperature DECIMAL(6, 2),
    tank2_vapor_pressure DECIMAL(6, 2),
    tank2_vapor_temperature DECIMAL(6, 2),
    tank3_vapor_pressure DECIMAL(6, 2),
    tank3_vapor_temperature DECIMAL(6, 2),
    tank4_vapor_pressure DECIMAL(6, 2),
    tank4_vapor_temperature DECIMAL(6, 2),
    data_source VARCHAR(100),
    original_datetime TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ship FOREIGN KEY (ship_id) REFERENCES d_ship(ship_id),
    CONSTRAINT fk_datetime FOREIGN KEY (datetime_id) REFERENCES d_calendar(datetime_id),
    CONSTRAINT unique_ship_datetime_key UNIQUE (ship_id, datetime_id)
);

CREATE INDEX idx_f_data_ship_id ON f_data(ship_id);
CREATE INDEX idx_f_data_datetime_id ON f_data(datetime_id);
CREATE INDEX idx_f_data_lat_lon ON f_data(latitude, longitude);
CREATE INDEX idx_d_ship_ship_id ON d_ship(ship_id);
CREATE INDEX idx_d_calendar_datetime ON d_calendar(year, month, day, hour, minute);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_d_ship_updated_at BEFORE UPDATE ON d_ship
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE d_ship IS 'Dimension table for ship information';
COMMENT ON TABLE d_calendar IS 'Dimension table for date/time dimension';
COMMENT ON TABLE f_data IS 'Fact table for ship tracking and sensor data';

COMMENT ON COLUMN d_ship.ship_id IS 'Ship identifier used as primary key';
COMMENT ON COLUMN d_calendar.datetime_id IS 'Complete timestamp used as primary key';
COMMENT ON COLUMN d_calendar.date IS 'Date component of timestamp';
COMMENT ON COLUMN d_calendar.hour IS 'Hour component of timestamp';
COMMENT ON COLUMN d_calendar.minute IS 'Minute component of timestamp';
COMMENT ON COLUMN f_data.fact_key IS 'Surrogate key for fact table records';