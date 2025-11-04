# Ship Tracking Data ETL Pipeline - Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Data Flow](#data-flow)
4. [Database Design](#database-design)
5. [Component Overview](#component-overview)
6. [Technology Stack](#technology-stack)
7. [Deployment & Infrastructure](#deployment--infrastructure)
8. [Data Models](#data-models)
9. [Query Interfaces](#query-interfaces)
10. [Extensibility & Future Enhancements](#extensibility--future-enhancements)

# Ship Tracking Data ETL Pipeline - Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Data Flow](#data-flow)
4. [Database Design](#database-design)
5. [Component Overview](#component-overview)
6. [Technology Stack](#technology-stack)
7. [Deployment & Infrastructure](#deployment--infrastructure)
8. [Data Models](#data-models)
9. [Query Interfaces](#query-interfaces)
10. [Extensibility & Future Enhancements](#extensibility--future-enhancements)

## Overview

This project implements a comprehensive **Ship Tracking Data ETL Pipeline** that processes maritime vessel sensor data from Excel files and transforms it into a structured, queryable format for analysis and visualization. The system employs a star schema database design optimized for analytical workloads with PostgreSQL as the primary database backend.

### Key Features
- **Multi-format Data Ingestion**: Processes XLSX files with embedded JSON-like data structures
- **Star Schema Design**: Optimized for analytical queries and reporting
- **PostgreSQL Focus**: Production-ready database with schema validation and integrity
- **Containerized Deployment**: Docker-based infrastructure with health monitoring
- **Advanced Data Validation**: Comprehensive sensor data validation with range checking
- **Bulk Loading Performance**: High-performance batch processing with execute_values
- **Error Resilience**: Robust error handling with transaction management
- **Historical Data Processing**: Handles temporal data with dimensional modeling

## System Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A1[data_sample_0.xlsx]
        A2[data_sample_1.xlsx]
        A3[...more xlsx files]
    end
    
    subgraph "ETL Pipeline"
        B1[etl_ship_data_with_schema.py]
        B2[Schema Validation]
        B3[Data Extraction]
        B4[Data Transformation]
        B5[Data Validation]
        B6[Bulk Loading]
    end
    
    subgraph "Database Layer"
        C1[(PostgreSQL 15)]
        C2[(Star Schema: d_ship, d_calendar, f_data)]
    end
    
    subgraph "Query Interfaces"
        D1[SQL Query Files]
        D2[geojson_query.sql]
        D3[check_duplicate_data.sql]
        D4[basic.sql]
    end
    
    subgraph "Application Layer"
        E1[main.py]
        E2[Demo Scripts]
        E3[Query Testing]
    end
    
    subgraph "Infrastructure"
        F1[Docker Compose]
        F2[PostgreSQL Container]
        F3[Health Monitoring]
        F4[Persistent Volumes]
    end
    
    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> B4
    B4 --> B5
    B5 --> B6
    B6 --> C1
    C1 --> C2
    C2 --> D1
    D1 --> D2
    D1 --> D3
    D1 --> D4
    D2 --> E1
    D3 --> E1
    D4 --> E1
    E1 --> E2
    E1 --> E3
    F1 --> F2
    F2 --> C1
    F3 --> F2
    F4 --> C1
```

## Data Flow

### 1. Data Extraction
- **Source Format**: XLSX files in `data/` directory
- **Sheet Structure**: Data organized in 'DATA' sheet
- **Data Structure**: Each row contains ship ID, timestamps, and JSON-like data strings
- **File Processing**: Automated discovery and processing of all .xlsx files with validation

### 2. Schema Validation
- **Database Schema Check**: Validates existence of required tables (d_ship, d_calendar, f_data)
- **Auto Schema Creation**: Creates schema from SQL file if missing
- **Connection Management**: PostgreSQL connection with error handling and rollback

### 3. Data Transformation
- **Data Parsing**: Converts JSON-like strings into structured objects using `ast.literal_eval()`
- **Data Validation**: Comprehensive range validation for all sensor data fields
- **Data Normalization**: Maps source fields to standardized database columns
- **Timestamp Processing**: Converts to proper datetime objects with timezone awareness
- **Error Resilience**: Logs validation errors and continues processing valid records

### 4. Bulk Loading
- **High Performance**: Uses PostgreSQL execute_values for efficient batch inserts
- **Batch Processing**: Processes data in configurable batch sizes (1000 records)
- **Duplicate Prevention**: Implements ON CONFLICT handling to avoid duplicates
- **Transaction Management**: Each batch committed independently for resilience
- **Progress Tracking**: Detailed logging of batch processing and statistics

## Database Design

### PostgreSQL Star Schema Architecture

The system implements a **single PostgreSQL database** with a traditional star schema design optimized for analytical workloads:

#### Fact Table: `f_data`
- **Primary Key**: `fact_key` (auto-incrementing surrogate key)
- **Natural Keys**: `ship_id`, `datetime_id` (composite unique constraint)
- **Measures**:
  - Position data: `latitude`, `longitude`
  - Environmental: `wind_direction`, `wind_speed`, `air_temperature`
  - Tank metrics: 5 tanks × (liquid_volume, max_volume, percentage, vapor_pressure, vapor_temperature)
- **Metadata**: `data_source`, `original_datetime`
- **Timestamps**: `created_at` for audit trail

#### Dimension Tables

**`d_ship`**
- **Primary Key**: `ship_id` (VARCHAR 50) - Uses actual ship ID as primary key
- **Attributes**: Ship identifier only
- **Purpose**: Ship master data with natural key approach
- **Timestamps**: `created_at`, `updated_at` with automatic trigger

**`d_calendar`**
- **Primary Key**: `datetime_id` (TIMESTAMP) - Uses complete timestamp as primary key
- **Attributes**: Date/time components (year, month, day, hour, minute)
- **Derived Metrics**: Quarter, week of year, day of week, weekend indicator
- **Purpose**: Time intelligence for analytical queries
- **Constraints**: Unique constraints on date components and full timestamp

### Database Features

#### Performance Optimizations
- **Indexing Strategy**:
  - `idx_f_data_ship_id`: Ship lookups
  - `idx_f_data_datetime_id`: Time-based queries
  - `idx_f_data_lat_lon`: Geographic queries
  - `idx_d_calendar_datetime`: Date component queries

#### Data Integrity
- **Foreign Key Constraints**: Ensures referential integrity between fact and dimensions
- **Unique Constraints**: Prevents duplicate ship-datetime combinations
- **Data Validation**: Application-level validation with range checking
- **Transaction Management**: ACID compliance with rollback capabilities

#### Schema Management
- **Auto Schema Creation**: Schema created automatically from SQL file if missing
- **Version Control**: Schema changes tracked through SQL file versioning
- **Migration Support**: Drop-and-recreate approach for schema updates

## Component Overview

### 1. ETL Pipeline (`etl_ship_data_with_schema.py`)
**Purpose**: Comprehensive ETL processing with schema management

**Key Functions**:
- `connect_to_db()`: PostgreSQL connection with automatic schema validation
- `create_database_schema()`: Automatic schema creation from SQL file
- `process_xlsx_file()`: File-level processing with comprehensive error handling
- `parse_data_column()`: JSON-like string parsing with data validation
- `validate_sensor_data()`: Range validation for all sensor measurements
- `get_or_create_ship_id()`: Ship dimension management
- `get_or_create_datetime_id()`: Calendar dimension management
- `load_data_to_postgres_bulk()`: High-performance bulk loading

**Advanced Features**:
- **Data Validation**: Comprehensive sensor data validation with configurable ranges
- **Bulk Processing**: Batch processing with execute_values for high performance
- **Error Resilience**: Transaction management with automatic rollback on failures
- **Progress Tracking**: Detailed logging and statistics for each processing step
- **Schema Auto-Management**: Automatic schema creation and validation
- **Duplicate Prevention**: ON CONFLICT handling for data integrity

### 2. Query Interface System
**Purpose**: SQL-based querying and analysis through dedicated query files

**Key Query Files**:
- `geojson_query.sql`: Geographic coordinate extraction for mapping
- `check_duplicate_data.sql`: Data quality validation and duplicate detection
- `basic.sql`: Fundamental ship tracking queries
- `create_postgresql_schema.sql`: Schema creation and management

### 3. Application Layer (`main.py`)
**Function**: Demonstration and testing coordinator

**Capabilities**:
- Database connection management
- Example query execution
- Data processing demonstrations
- Interactive testing framework

### 4. Container Infrastructure (`docker-compose.yml`)
**Services**:
- **PostgreSQL 15**: Production-ready database with optimized configuration
- **Health Monitoring**: Built-in health checks and automatic restart policies
- **Volume Management**: Persistent data storage with named volumes
- **Network Isolation**: Dedicated bridge network for security
- **Environment Management**: Environment variable configuration via .env files

## Technology Stack

### Core Technologies
- **Python 3.x**: Primary programming language with modern features
- **Pandas**: High-performance data manipulation and analysis
- **Psycopg2**: Native PostgreSQL adapter for optimal performance
- **SQLAlchemy**: Database ORM and connection management
- **OpenPyXL**: Excel file reading and processing

### Infrastructure
- **Docker**: Containerization platform for consistent deployment
- **PostgreSQL 15**: Production database engine with advanced features
- **Docker Compose**: Multi-container orchestration and service management

### Data Processing & Validation
- **AST Parsing**: Safe JSON-like string parsing with ast.literal_eval()
- **DateTime Processing**: Temporal data handling with timezone awareness
- **Data Validation**: Custom validation framework with range checking
- **Bulk Operations**: PostgreSQL execute_values for high-performance batch loading

### Configuration & Environment
- **python-dotenv**: Environment variable management
- **Context Managers**: Safe resource management and cleanup
- **Logging Framework**: Comprehensive logging with file and console handlers

### Development & Quality
- **Exception Handling**: Robust error management with user-friendly messages
- **Transaction Management**: ACID compliance with automatic rollback
- **Progress Tracking**: Detailed logging and processing statistics
- **Code Documentation**: Comprehensive docstrings and inline comments

## Deployment & Infrastructure

### Development Environment
```bash
# Python virtual environment setup
./setup_venv.sh

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env  # if exists
# Edit .env with database credentials

# Run demonstration
python main.py
```

### Production Environment
```bash
# Start PostgreSQL container
docker-compose up -d

# Wait for container initialization
docker-compose logs -f postgres

# Run ETL pipeline with schema auto-creation
python etl_ship_data_with_schema.py

# Verify container health and data
docker-compose ps
```

### Environment Configuration
- **Database URL**: Configured via DATABASE_URL environment variable
- **Data Path**: Configured via DATA_FOLDER_PATH environment variable
- **PostgreSQL**: Containerized on port 5432
- **Database**: Auto-created database with schema validation
- **Schema Management**: Auto-initialized via create_postgresql_schema.sql
- **Volumes**: Persistent data storage with Docker volumes

### Configuration Management
```bash
# Environment file (.env) example
DATABASE_URL=postgresql://vesseluser:vesselpass123@localhost:5432/vesseldb
DATA_FOLDER_PATH=./data
```

## Data Models

### Complete Ship Tracking Data Schema
```sql
-- Dimension: Ships (Natural Key Approach)
CREATE TABLE d_ship (
    ship_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Calendar/Time
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
    UNIQUE(year, month, day, hour, minute)
);

-- Fact: Ship Data with Comprehensive Sensor Measurements
CREATE TABLE f_data (
    fact_key SERIAL PRIMARY KEY,
    ship_id VARCHAR(50) NOT NULL,
    datetime_id TIMESTAMP NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    -- Environmental measures
    wind_direction DECIMAL(5, 2),
    wind_speed DECIMAL(5, 2),
    air_temperature DECIMAL(5, 2),
    -- Tank measures (5 tanks with full metrics)
    tank0_liquid_volume DECIMAL(10, 2),
    tank0_max_volume DECIMAL(10, 2),
    tank0_percentage DECIMAL(5, 2),
    tank1_liquid_volume DECIMAL(10, 2),
    tank1_max_volume DECIMAL(10, 2),
    tank1_percentage DECIMAL(5, 2),
    -- ... additional tanks 2, 3, 4
    tank0_vapor_pressure DECIMAL(6, 2),
    tank0_vapor_temperature DECIMAL(6, 2),
    -- ... additional vapor measurements
    data_source VARCHAR(100),
    original_datetime TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ship FOREIGN KEY (ship_id) REFERENCES d_ship(ship_id),
    CONSTRAINT fk_datetime FOREIGN KEY (datetime_id) REFERENCES d_calendar(datetime_id),
    CONSTRAINT unique_ship_datetime_key UNIQUE (ship_id, datetime_id)
);

-- Performance Indexes
CREATE INDEX idx_f_data_ship_id ON f_data(ship_id);
CREATE INDEX idx_f_data_datetime_id ON f_data(datetime_id);
CREATE INDEX idx_f_data_lat_lon ON f_data(latitude, longitude);
```

### Data Mapping & Validation
Source Excel columns are mapped to database fields with comprehensive validation:
- `data` column → Parsed JSON → Individual sensor measurements with range validation
- `id_ship` → `ship_id` in dimension (natural key approach)
- `datetime` → `datetime_id` as primary key in calendar dimension
- **Validation Rules**:
  - Latitude: -90 to +90 degrees
  - Longitude: -180 to +180 degrees
  - Wind direction: 0 to 360 degrees
  - Wind speed: 0 to 200 units
  - Air temperature: -50 to 60°C
  - Tank percentages: 0 to 100%
  - Tank volumes: 0 to 999999.99 units
  - Vapor measurements: pressure 0-9999.99, temperature -200 to 200°C

## Query Interfaces

### ETL Pipeline Functions
```python
# Environment setup
dotenv.load_dotenv(".env")
db_conn_string = os.getenv('DATABASE_URL')
data_folder_path = os.getenv('DATA_FOLDER_PATH')

# Run complete ETL process
db_conn = connect_to_db(db_conn_string)
etl(db_conn=db_conn, data_path=data_folder_path)

# Process individual files with validation
df_data = process_xlsx_file(excel_file_path=Path("data/sample.xlsx"))
records_loaded = load_data_to_postgres(processed_df=df_data, conn=db_conn)
```

### SQL Query Files
**Direct SQL Execution:**
```sql
-- Geographic data for mapping applications
SELECT '[[' || longitude::text || ',' || latitude::text || ']]' as geo
FROM f_data;

-- Data quality validation
SELECT ship_key, calendar_key, COUNT(*) as duplicate_count
FROM f_data
GROUP BY ship_key, calendar_key
HAVING COUNT(*) > 1;
```

### Database Connection Pattern
```python
# Safe connection management
conn = psycopg2.connect(db_connection_string)
try:
    # Query execution with context management
    with get_db_cursor(conn) as cursor:
        cursor.execute("SELECT * FROM f_data WHERE ship_id = %s", (ship_id,))
        results = cursor.fetchall()
finally:
    conn.close()
```

## Extensibility & Future Enhancements

### Immediate Opportunities
1. **Real-time Data Ingestion**: Add streaming data sources (IoT sensors, AIS feeds)
2. **Advanced Analytics**: Machine learning for predictive maintenance, route optimization
3. **API Layer**: RESTful API for external system integration
4. **Dashboard Interface**: Web-based visualization dashboard
5. **Data Quality Framework**: Automated validation and cleansing pipelines

### Scalability Considerations
1. **Database Optimization**:
   - Table partitioning for large temporal datasets
   - Materialized views for common analytical queries
   - Read replicas for query load distribution
2. **ETL Parallelization**: Multi-process file processing for better throughput
3. **Data Lake Integration**: HDFS/S3 for historical data storage and archiving
4. **Microservices Architecture**: Service decomposition for better maintainability

### Integration Capabilities
1. **Maritime APIs**: Integration with AIS (Automatic Identification System) data
2. **Weather Services**: Enhanced environmental data correlation and forecasting
3. **Port Systems**: Integration with port operational data and scheduling
4. **Fleet Management**: Connection to vessel management and maintenance systems

### Performance Optimization
1. **Query Optimization**: Advanced indexing strategies and query plan analysis
2. **Caching Layer**: Redis/Memcached for frequently accessed data
3. **Data Compression**: Columnar storage for analytical workloads
4. **Monitoring & Alerting**: Database performance monitoring and automated alerts

---

**Document Version**: 2.0
**Last Updated**: 2025-11-04
**Architecture Review**: Updated to reflect current implementation
**Key Changes**: PostgreSQL-only architecture, enhanced data validation, bulk loading performance, schema auto-management