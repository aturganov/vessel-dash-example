#!/usr/bin/env python3
"""
Скрипт для данных отслеживания судов
Извлекает данные из файлов XLSX и загружает в PostgreSQL звездную схему
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values  
from sqlalchemy import create_engine
import ast
import dotenv

from typing import Dict, List, Optional, Tuple, Any
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[ 
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_database_schema(conn: psycopg2.extensions.connection) -> bool:
    """Создание схемы базы данных из SQL файла"""
    schema_file_path = Path(__file__).parent / "create_postgresql_schema.sql"
    
    if not schema_file_path.exists():
        logger.error(f"Файл схемы не найден: {schema_file_path}")
        return False
    
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as schema_file:
            schema_sql = schema_file.read()
        
        # Use PostgreSQL's native execution instead of naive parsing
        # This handles dollar-quoted strings, multi-line statements, and complex syntax correctly
        with conn.cursor() as cursor:
            cursor.execute(schema_sql)
            logger.info("Схема базы данных успешно создана с использованием PostgreSQL синтаксиса")
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Не удалось создать схему базы данных: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False

def validate_database_schema(conn: psycopg2.extensions.connection) -> bool:
    """Проверка существования необходимых таблиц базы данных"""
    required_tables = ['d_ship', 'd_calendar', 'f_data']
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            if missing_tables:
                logger.warning(f"Отсутствуют обязательные таблицы базы данных: {missing_tables}")
                return False
            
            logger.info("Проверка схемы базы данных пройдена")
            return True
    except Exception as e:
        logger.error(f"Не удалось проверить схему базы данных: {e}")
        return False

@contextmanager
def get_db_cursor(conn: psycopg2.extensions.connection, cursor_factory=None):
    """Контекстный менеджер для безопасного создания и очистки курсора"""
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=cursor_factory)
        yield cursor
    except Exception as e:
        logger.error(f"Ошибка курсора базы данных: {e}")
        try:
            conn.rollback()
        except:
            pass
        raise
    finally:
        if cursor:
            cursor.close()

def connect_to_db(db_connection_string) -> psycopg2.extensions.connection:
    """Создание подключения к базе данных с проверкой схемы"""
    try:
        conn = psycopg2.connect(db_connection_string)
        logger.info("Успешно подключено к базе данных PostgreSQL")
        
        # Проверка существования схемы
        if not validate_database_schema(conn):
            logger.info("Попытка создания отсутствующей схемы базы данных...")
            if create_database_schema(conn):
                logger.info("Схема базы данных создана и проверена успешно")
            else:
                raise RuntimeError("Не удалось создать схему базы данных")
        
        return conn
    except Exception as e:
        logger.error(f"Не удалось подключиться к базе данных: {e}")
        raise

def validate_sensor_data(data_dict: Dict, row_identifier: Any) -> Dict:
    """Валидация данных сенсоров с проверкой диапазонов значений"""
    validation_rules = {
        'LAT': ('latitude', -90, 90, 6),
        'LON': ('longitude', -180, 180, 6),
        'WINDIR': ('wind_direction', 0, 360, 2),
        'WINSPE': ('wind_speed', 0, 200, 2),
        'AIR_TEMP_AUT': ('air_temperature', -50, 60, 2),
        'CTNK0_LIQ_VOL': ('tank0_liquid_volume', 0, 999999.99, 2),
        'CTNK0_MAX_VOL': ('tank0_max_volume', 0, 999999.99, 2),
        'CTNK0_MAX_PERC': ('tank0_percentage', 0, 100, 2),
        'CTNK1_LIQ_VOL': ('tank1_liquid_volume', 0, 999999.99, 2),
        'CTNK1_MAX_VOL': ('tank1_max_volume', 0, 999999.99, 2),
        'CTNK1_PERC': ('tank1_percentage', 0, 100, 2),
        'CTNK2_LIQ_VOL': ('tank2_liquid_volume', 0, 999999.99, 2),
        'CTNK2_MAX_VOL': ('tank2_max_volume', 0, 999999.99, 2),
        'CTNK2_PERC': ('tank2_percentage', 0, 100, 2),
        'CTNK3_LIQ_VOL': ('tank3_liquid_volume', 0, 999999.99, 2),
        'CTNK3_MAX_VOL': ('tank3_max_volume', 0, 999999.99, 2),
        'CTNK3_PERC': ('tank3_percentage', 0, 100, 2),
        'CTNK4_LIQ_VOL': ('tank4_liquid_volume', 0, 999999.99, 2),
        'CTNK4_MAX_VOL': ('tank4_max_volume', 0, 999999.99, 2),
        'CTNK4_PERC': ('tank4_percentage', 0, 100, 2),
        'CTNK0_VAP_PRES': ('tank0_vapor_pressure', 0, 9999.99, 2),
        'CTNK0_VAP_TEMP': ('tank0_vapor_temperature', -200, 200, 2),
        'CTNK1_VAP_PRES': ('tank1_vapor_pressure', 0, 9999.99, 2),
        'CTNK1_VAP_TEMP': ('tank1_vapor_temperature', -200, 200, 2),
        'CTNK2_VAP_PRES': ('tank2_vapor_pressure', 0, 9999.99, 2),
        'CTNK2_VAP_TEMP': ('tank2_vapor_temperature', -200, 200, 2),
        'CTNK3_VAP_PRES': ('tank3_vapor_pressure', 0, 9999.99, 2),
        'CTNK3_VAP_TEMP': ('tank3_vapor_temperature', -200, 200, 2),
        'CTNK4_VAP_PRES': ('tank4_vapor_pressure', 0, 9999.99, 2),
        'CTNK4_VAP_TEMP': ('tank4_vapor_temperature', -200, 200, 2)
    }
    
    validated_data = {}
    validation_errors = 0
    row_id_str = str(row_identifier)
    
    for source_key, (target_field, min_val, max_val, precision) in validation_rules.items():
        if source_key in data_dict:
            try:
                value = data_dict[source_key]
                
                # Skip None/NaN values
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    continue
                    
                # Convert to float and validate range
                numeric_value = float(value)
                
                if not (min_val <= numeric_value <= max_val):
                    logger.warning(f"Строка {row_id_str}: {target_field}={numeric_value} вне диапазона [{min_val}, {max_val}]")
                    validation_errors += 1
                    continue
                
                # Round to specified precision
                rounded_value = round(numeric_value, precision)
                validated_data[source_key] = rounded_value
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Строка {row_id_str}: Не удалось валидировать {source_key}: {e}")
                validation_errors += 1
                continue
    
    if validation_errors > 0:
        logger.info(f"Строка {row_id_str}: Валидация завершена с {validation_errors} ошибками")
    
    return validated_data

def parse_data_column(data_str: str, row_identifier: Any = "0") -> Dict:
    """Парсинг колонки данных, которая содержит JSON-подобную строку с валидацией"""
    try:
        # Удаление лишних кавычек и очистка строки
        if isinstance(data_str, str):
            data_str = data_str.strip('"').replace('""', '"')
            # Использование ast.literal_eval для безопасного парсинга словаря
            parsed_data = ast.literal_eval(data_str)
        elif isinstance(data_str, dict):
            parsed_data = data_str
        else:
            logger.warning(f"Строка {str(row_identifier)}: Неожиданный тип данных: {type(data_str)}")
            return {}
        
        # Apply data validation
        validated_data = validate_sensor_data(parsed_data, row_identifier)
        
        if not validated_data:
            logger.warning(f"Строка {str(row_identifier)}: После валидации данные отсутствуют")
            return {}
            
        return validated_data
        
    except json.JSONDecodeError as e:
        logger.warning(f"Строка {str(row_identifier)}: Ошибка парсинга JSON: {e}")
        return {}
    except ValueError as e:
        logger.warning(f"Строка {str(row_identifier)}: Ошибка валидации данных: {e}")
        return {}
    except Exception as e:
        logger.warning(f"Строка {str(row_identifier)}: Неожиданная ошибка парсинга: {e}")
        return {}

def get_excel_files(folder_path: str, file_extensions: Optional[List[str]] = None) -> List[Path]:
    """
    Получение списка файлов Excel с кэшированием и валидацией
    Возвращает:
        Список объектов Path для файлов Excel, отсортированных и без дубликатов
        
    """
    # Константы и конфигурация
    DEFAULT_EXCEL_EXTENSIONS = ['.xlsx', '.xls']
    CACHE_DURATION_SECONDS = 300  # 5 минут
    
    if file_extensions is None:
        file_extensions = DEFAULT_EXCEL_EXTENSIONS
    
    if not file_extensions:
        raise ValueError("file_extensions не может быть пустым")
    
    # Проверка и нормализация входного пути
    search_path = Path(folder_path).resolve()  # Преобразование в абсолютный путь
    
    try:
        # Расширенная проверка пути с определенными типами ошибок
        if not search_path.exists():
            raise FileNotFoundError(f"Папка с данными '{search_path}' не существует")
        
        if not search_path.is_dir():
            raise NotADirectoryError(f"'{search_path}' не является директорией, но файл размером '{search_path.stat().st_size}' байт")
        
        if not os.access(search_path, os.R_OK):
            raise PermissionError(f"Нет прав на чтение директории '{search_path}'")
        
        # Оптимизация производительности: Использование glob для эффективного поиска файлов
        excel_files = []
        for extension in file_extensions:
            # Использование glob для сопоставления шаблонов (более эффективно чем ручная фильтрация)
            pattern = f"*{extension}"
            matching_files = list(search_path.glob(pattern))
            excel_files.extend(matching_files)
        
        # Дедупликация
        excel_files = sorted(list(set(excel_files)))
        
        # Проверка содержимого файла Excel
        valid_files = []
        for file_path in excel_files:
            try:
                if file_path.is_file() and file_path.stat().st_size > 0:
                    valid_files.append(file_path)
                else:
                    logger.warning(f"Пропуск пустого или недопустимого файла: {file_path}")
            except (OSError, PermissionError) as e:
                logger.warning(f"Невозможно получить доступ к файлу '{file_path}': {e}")
                continue
        
        excel_files = valid_files
        
        # Логирование количества файлов и загрузки
        if excel_files:
            logger.info(f"Найдено {len(excel_files)} валидных файлов Excel в '{search_path}':")
            for file_path in excel_files[:5]:  # Логирование первых 5 файлов
                logger.info(f"  - {file_path.name}")
            if len(excel_files) > 5:
                logger.info(f"  ... и еще {len(excel_files) - 5} файлов")
        else:
            logger.warning(f"Файлы Excel не найдены в '{search_path}' с расширениями: {file_extensions}")
        
        return excel_files
    
    # Логирование ошибок
    except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
        # Логирование и повторный выброс специфичных исключений
        logger.error(f"Проверка пути не удалась для '{search_path}': {e}")
        raise
    except Exception as e:
        # Перехват всех неожиданных ошибок
        logger.error(f"Неожиданная ошибка при доступе к файлам Excel из '{search_path}': {e}")
        raise RuntimeError(f"Не удалось сканировать директорию '{search_path}': {e}") from e

def validate_input_data(row_data: Dict, required_columns: List[str]) -> bool:
    """Проверка обязательных колонок в данных"""
    missing_columns = [col for col in required_columns if col not in row_data]
    if missing_columns:
        logger.warning(f"Отсутствуют обязательные колонки: {missing_columns}")
        return False
    return True

def process_xlsx_file(excel_file_path: Path) -> pd.DataFrame:
    """Обработка одного файла XLSX с оптимизацией памяти и улучшенной обработкой ошибок"""
    logger.info(f"Обработка файла XLSX: {excel_file_path}")
    
    required_columns = ['id_ship', 'datetime', 'data']
    file_size_mb = excel_file_path.stat().st_size / (1024 * 1024)
    logger.info(f"Размер файла: {file_size_mb:.2f} MB")
    
    try:
        # Memory optimization: Simple file reading for now
        df = pd.read_excel(excel_file_path, sheet_name='DATA')
        
        logger.info(f"Загружено {len(df)} строк из {excel_file_path}")
        
        # Обработка каждой строки с улучшенной валидацией
        processed_rows = []
        validation_errors = 0
        processing_errors = 0
        
        for idx, row in df.iterrows():
            try:
                # Validate required columns
                if not validate_input_data(row.to_dict(), required_columns):
                    validation_errors += 1
                    continue
                
                # Sanitize input data
                ship_id = str(row['id_ship']).strip() if pd.notna(row['id_ship']) else None
                datetime_str = str(row['datetime']).strip() if pd.notna(row['datetime']) else None
                data_str = str(row['data']) if pd.notna(row['data']) else None
                
                if not all([ship_id, datetime_str, data_str]):
                    logger.warning(f"Строка {idx}: Пустые обязательные значения")
                    validation_errors += 1
                    continue
                
                # Парсинг колонки данных с валидацией (data_str уже проверен как не-None)
                data_dict = parse_data_column(str(data_str), idx)
                
                if not data_dict:
                    logger.warning(f"Данные не разобраны для строки {idx} в {excel_file_path}")
                    validation_errors += 1
                    continue
                
                # Создание обработанной строки с безопасным извлечением данных
                processed_row = {
                    'id_ship': ship_id,
                    'datetime': datetime_str,
                    'datetime_created': str(row['datetime_created']).strip() if pd.notna(row['datetime_created']) else None,
                    'latitude': data_dict.get('LAT'),
                    'longitude': data_dict.get('LON'),
                    'wind_direction': data_dict.get('WINDIR'),
                    'wind_speed': data_dict.get('WINSPE'),
                    'air_temperature': data_dict.get('AIR_TEMP_AUT'),
                    'tank0_liquid_volume': data_dict.get('CTNK0_LIQ_VOL'),
                    'tank0_max_volume': data_dict.get('CTNK0_MAX_VOL'),
                    'tank0_percentage': data_dict.get('CTNK0_MAX_PERC'),
                    'tank1_liquid_volume': data_dict.get('CTNK1_LIQ_VOL'),
                    'tank1_max_volume': data_dict.get('CTNK1_MAX_VOL'),
                    'tank1_percentage': data_dict.get('CTNK1_PERC'),
                    'tank2_liquid_volume': data_dict.get('CTNK2_LIQ_VOL'),
                    'tank2_max_volume': data_dict.get('CTNK2_MAX_VOL'),
                    'tank2_percentage': data_dict.get('CTNK2_PERC'),
                    'tank3_liquid_volume': data_dict.get('CTNK3_LIQ_VOL'),
                    'tank3_max_volume': data_dict.get('CTNK3_MAX_VOL'),
                    'tank3_percentage': data_dict.get('CTNK3_PERC'),
                    'tank4_liquid_volume': data_dict.get('CTNK4_LIQ_VOL'),
                    'tank4_max_volume': data_dict.get('CTNK4_MAX_VOL'),
                    'tank4_percentage': data_dict.get('CTNK4_PERC'),
                    'tank0_vapor_pressure': data_dict.get('CTNK0_VAP_PRES'),
                    'tank0_vapor_temperature': data_dict.get('CTNK0_VAP_TEMP'),
                    'tank1_vapor_pressure': data_dict.get('CTNK1_VAP_PRES'),
                    'tank1_vapor_temperature': data_dict.get('CTNK1_VAP_TEMP'),
                    'tank2_vapor_pressure': data_dict.get('CTNK2_VAP_PRES'),
                    'tank2_vapor_temperature': data_dict.get('CTNK2_VAP_TEMP'),
                    'tank3_vapor_pressure': data_dict.get('CTNK3_VAP_PRES'),
                    'tank3_vapor_temperature': data_dict.get('CTNK3_VAP_TEMP'),
                    'tank4_vapor_pressure': data_dict.get('CTNK4_VAP_PRES'),
                    'tank4_vapor_temperature': data_dict.get('CTNK4_VAP_TEMP'),
                    'data_source': str(excel_file_path.name)
                }
                processed_rows.append(processed_row)
                
            except Exception as e:
                logger.error(f"Ошибка обработки строки {idx} в {excel_file_path}: {e}")
                processing_errors += 1
                continue
        
        # Логирование статистики обработки
        total_rows = len(df)
        processed_count = len(processed_rows)
        success_rate = (processed_count / total_rows * 100) if total_rows > 0 else 0
        
        logger.info(f"""
        Статистика обработки файла {excel_file_path.name}:
        - Всего строк: {total_rows}
        - Успешно обработано: {processed_count}
        - Ошибки валидации: {validation_errors}
        - Ошибки обработки: {processing_errors}
        - Процент успеха: {success_rate:.1f}%
        """)
        
        processed_df = pd.DataFrame(processed_rows)
        return processed_df
        
    except Exception as e:
        logger.error(f"Критическая ошибка обработки файла {excel_file_path}: {e}")
        return pd.DataFrame()

def get_or_create_ship_id(conn: psycopg2.extensions.connection, ship_id: str) -> str:
    """Получение существующего ship_id или создание новой записи в d_ship"""
    with get_db_cursor(conn) as cursor:
        # Попытка получить существующий ship_id
        cursor.execute("SELECT ship_id FROM d_ship WHERE ship_id = %s", (ship_id,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Создание новой записи о судне
        cursor.execute(
            "INSERT INTO d_ship (ship_id) VALUES (%s)",
            (ship_id,)
        )
        conn.commit()
        logger.info(f"Создана новая запись о судне для {ship_id}")
        return ship_id

def get_or_create_datetime_id(conn: psycopg2.extensions.connection, timestamp: datetime) -> datetime:
    """Получение существующего datetime_id или создание новой записи в d_calendar"""
    with get_db_cursor(conn) as cursor:
        # Попытка получить существующий datetime_id для этой даты и времени
        cursor.execute("""
            SELECT datetime_id FROM d_calendar
            WHERE year = %s AND month = %s AND day = %s AND hour = %s AND minute = %s
        """, (timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute))
        result = cursor.fetchone()
        
        if result:
            return result[0]  # Return the timestamp directly
        
        # Вычисление дополнительных полей даты
        quarter = (timestamp.month - 1) // 3 + 1
        week_of_year = timestamp.isocalendar()[1]
        day_of_week = timestamp.weekday() + 1  # Понедельник=1, Воскресенье=7
        is_weekend = day_of_week >= 6
        
        # Создание новой записи календаря с datetime_id как TIMESTAMP primary key
        cursor.execute("""
            INSERT INTO d_calendar (
                datetime_id, date, year, month, day, hour, minute,
                quarter, week_of_year, day_of_week, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (timestamp, timestamp.date(), timestamp.year, timestamp.month, timestamp.day,
                timestamp.hour, timestamp.minute, quarter, week_of_year,
                day_of_week, is_weekend))
        
        conn.commit()
        logger.info(f"Создана новая запись календаря для {timestamp}")
        return timestamp  # Return the timestamp directly

def load_data_to_postgres(processed_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> int:
    """Bulk loading данных в PostgreSQL с использованием execute_values для высокой производительности"""
    return load_data_to_postgres_bulk(processed_df, conn)
    
def prepare_bulk_data(processed_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> List[Tuple]:
    """Подготовка данных для bulk loading с получением dimension keys"""
    bulk_data = []
    
    for idx, row in processed_df.iterrows():
        try:
            # Парсинг временной метки с валидацией
            timestamp = pd.to_datetime(row['datetime'])
            if pd.isna(timestamp):
                logger.warning(f"Недействительная временная метка для строки {idx}")
                continue
            
            # Получение или создание ship_id
            ship_id = get_or_create_ship_id(conn, row['id_ship'])
            
            # Получение или создание datetime_id
            datetime_id = get_or_create_datetime_id(conn, timestamp)
            
            # Подготовка записи для bulk insert
            bulk_record = (
                ship_id, datetime_id,
                row.get('latitude'), row.get('longitude'),
                row.get('wind_direction'), row.get('wind_speed'), row.get('air_temperature'),
                row.get('tank0_liquid_volume'), row.get('tank0_max_volume'), row.get('tank0_percentage'),
                row.get('tank1_liquid_volume'), row.get('tank1_max_volume'), row.get('tank1_percentage'),
                row.get('tank2_liquid_volume'), row.get('tank2_max_volume'), row.get('tank2_percentage'),
                row.get('tank3_liquid_volume'), row.get('tank3_max_volume'), row.get('tank3_percentage'),
                row.get('tank4_liquid_volume'), row.get('tank4_max_volume'), row.get('tank4_percentage'),
                row.get('tank0_vapor_pressure'), row.get('tank0_vapor_temperature'),
                row.get('tank1_vapor_pressure'), row.get('tank1_vapor_temperature'),
                row.get('tank2_vapor_pressure'), row.get('tank2_vapor_temperature'),
                row.get('tank3_vapor_pressure'), row.get('tank3_vapor_temperature'),
                row.get('tank4_vapor_pressure'), row.get('tank4_vapor_temperature'),
                row.get('data_source'), row.get('datetime')
            )
            
            bulk_data.append(bulk_record)
            
        except Exception as e:
            logger.error(f"Ошибка подготовки данных для строки {idx}: {e}")
            continue
    
    return bulk_data

def load_data_to_postgres_bulk(processed_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> int:
    """Bulk loading данных в PostgreSQL с использованием execute_values для высокой производительности"""
    if processed_df.empty:
        logger.warning("Нет данных для загрузки")
        return 0
    
    records_loaded = 0
    batch_size = 1000
    
    try:
        # Обработка данных батчами
        for batch_start in range(0, len(processed_df), batch_size):
            batch_end = min(batch_start + batch_size, len(processed_df))
            batch_df = processed_df.iloc[batch_start:batch_end]
            
            logger.info(f"Обработка bulk батча {batch_start//batch_size + 1}: строки {batch_start}-{batch_end}")
            
            # Подготовка данных для bulk insert
            bulk_data = prepare_bulk_data(batch_df, conn)
            
            if not bulk_data:
                logger.warning(f"Батч {batch_start//batch_size + 1}: нет валидных данных для загрузки")
                continue
            
            # Bulk insert с использованием execute_values и обработкой дубликатов
            with get_db_cursor(conn) as cursor:
                try:
                    execute_values(cursor, """
                        INSERT INTO f_data (
                            ship_id, datetime_id, latitude, longitude,
                            wind_direction, wind_speed, air_temperature,
                            tank0_liquid_volume, tank0_max_volume, tank0_percentage,
                            tank1_liquid_volume, tank1_max_volume, tank1_percentage,
                            tank2_liquid_volume, tank2_max_volume, tank2_percentage,
                            tank3_liquid_volume, tank3_max_volume, tank3_percentage,
                            tank4_liquid_volume, tank4_max_volume, tank4_percentage,
                            tank0_vapor_pressure, tank0_vapor_temperature,
                            tank1_vapor_pressure, tank1_vapor_temperature,
                            tank2_vapor_pressure, tank2_vapor_temperature,
                            tank3_vapor_pressure, tank3_vapor_temperature,
                            tank4_vapor_pressure, tank4_vapor_temperature,
                            data_source, original_datetime
                        ) VALUES %s
                        ON CONFLICT (ship_id, datetime_id) DO NOTHING
                    """, bulk_data, page_size=len(bulk_data))
                    
                    records_loaded += len(bulk_data)
                    logger.info(f"Батч {batch_start//batch_size + 1}: обработано {len(bulk_data)} записей (дубликаты автоматически пропущены)")
                    
                except psycopg2.IntegrityError as e:
                    logger.error(f"Ошибка целостности базы данных в батче {batch_start//batch_size + 1}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Ошибка bulk загрузки батча {batch_start//batch_size + 1}: {e}")
                    continue
            
            # Коммит каждого батча
            conn.commit()
            logger.info(f"Зафиксирован bulk батч {batch_start//batch_size + 1}")
        
        logger.info(f"Успешно загружено {records_loaded} записей в PostgreSQL через bulk операции")
        return records_loaded
        
    except Exception as e:
        logger.error(f"Критическая ошибка в load_data_to_postgres: {e}")
        try:
            conn.rollback()
            logger.info("Откат базы данных завершен")
        except Exception as rollback_error:
            logger.error(f"Не удалось откатить изменения в базе данных: {rollback_error}")
        raise RuntimeError(f"ETL процесс не удался после загрузки {records_loaded} записей") from e

def etl(db_conn, data_path):
    """ETL функция с обработкой ошибок"""
    try:
        # Получение списка файлов Excel
        excel_list_path = get_excel_files(data_path)
        total_files = len(excel_list_path)
        total_records = 0
        
        logger.info(f"Запуск ETL процесса для {total_files} файлов")
        
        for i, excel_file_path in enumerate(excel_list_path, 1):
            logger.info(f"Обработка файла {i}/{total_files}: {excel_file_path.name}")
            
            try:
                # Парсинг данных из Excel
                df_data = process_xlsx_file(excel_file_path=excel_file_path)
                
                if not df_data.empty:
                    # Сохранение данных в базу данных
                    records_loaded = load_data_to_postgres(processed_df=df_data, conn=db_conn)
                    total_records += records_loaded
                    logger.info(f"Файл {i}/{total_files} завершен: загружено {records_loaded} записей")
                else:
                    logger.warning(f"Файл {i}/{total_files} пропущен: нет валидных данных")
                    
            except Exception as file_error:
                logger.error(f"Ошибка обработки файла {i}/{total_files}: {file_error}")
                continue
        
        logger.info(f"ETL процесс завершен. Всего загружено записей: {total_records}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в ETL процессе: {e}")
        raise

if __name__ == "__main__":
    """Инициализация переменных окружения, подключения к базе данных и папки загрузки Excel"""
    dotenv.load_dotenv(".env")
    db_conn_string = os.getenv('DATABASE_URL')
    data_folder_path = os.getenv('DATA_FOLDER_PATH')
    
    if not db_conn_string:
        raise ValueError(f"Нет значения DATABASE_URL в файле .env")
    if not data_folder_path:
        raise ValueError(f"Нет значения DATA_FOLDER_PATH в файле .env")
    
    # Инициализация подключения к базе данных с проверкой схемы
    try:
        logger.info("Инициализация подключения к базе данных...")
        db_conn = connect_to_db(db_conn_string)
        
        # Запуск ETL процесса
        logger.info("Запуск ETL процесса...")
        etl(db_conn=db_conn, data_path=data_folder_path)
        
        # Очистка
        db_conn.close()
        logger.info("ETL процесс успешно завершен")
        
    except Exception as e:
        logger.error(f"ETL процесс не удался: {e}")
        raise