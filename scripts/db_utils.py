import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def create_db_engine(config_path='/home/user/workshop3_ml_datastreaming/config/credentials.json'):
    """
    Creates and tests a SQLAlchemy database engine using credentials from a JSON config file.

    Rads database connection parameters (username, password, host, etc.) from a specified
    JSON configuration file, creates a SQLAlchemy engine instance, and verifies the connection
    by establishing a test connection to the database.

    Parameters:
        config_path (str): Path to the JSON configuration file containing the database
                          credentials in key-value format.

    Returns:
        sqlalchemy.engine.base.Engine: A configured SQLAlchemy engine object if the connection
                                      is successful, None if the connection fails.

    Raises:
        FileNotFoundError: If the specified config file doesn't exist.
        JSONDecodeError: If the config file contains invalid JSON.
        KeyError: If required credentials are missing from the config file.
    """
    try:
      
        with open(config_path) as f:
            config = json.load(f)

        db_config = config['database']

        conn_str = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )

        engine = create_engine(conn_str)

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            print("✅ Connection successful!")

        return engine

    except (FileNotFoundError, KeyError, SQLAlchemyError) as e:
        print(f"❌ Connection aborted {e}")
        return None


def create_table(engine):
    """Creates a table in the database if it doesn't already exist.
    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object for database connection.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS happiness_data (
                    id SERIAL PRIMARY KEY,
                    country VARCHAR(100),
                    year INTEGER,
                    happiness_score FLOAT,
                    gdp_per_capita FLOAT,
                    social_support FLOAT,
                    life_expectancy FLOAT,
                    freedom FLOAT,
                    trust FLOAT,
                    generosity FLOAT
                );
            """))
            conn.commit()
            print("Tabla happiness_data creada o ya existente.")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")
        raise


def insert_data(df, engine):
    """Inserts data into the happiness_data table in the database.
    Args:
        df (pd.DataFrame): DataFrame containing the data to be inserted.
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object for database connection.
    """
    try:
        df.to_sql('happiness_data', engine, if_exists='append', index=False)
        print(f"Insert {len(df)} files in happiness_data.")
    except Exception as e:
        print(f"data insert failed: {e}")
        raise


def execute_database_pipeline(df=None, config_path='/home/user/workshop3_ml_datastreaming/config/credentials.json'):
    """
    Executes the complete database pipeline:
    1. Creates a database engine using credentials from config file
    2. Creates the happiness_data table if it doesn't exist
    3. Inserts provided DataFrame data into the table (if DataFrame provided)

    Parameters:
        df (pd.DataFrame, optional): DataFrame containing happiness data to insert. 
                                   If None, only creates engine and table. Default None.
        config_path (str): Path to JSON config file with database credentials.
                          Default '/home/user/workshop3_ml_datastreaming/config/credentials.json'

    Returns:
        tuple: (sqlalchemy.engine.base.Engine, bool) where:
               - Engine is the database connection engine
               - bool indicates whether all operations succeeded

    Raises:
        FileNotFoundError: If config file is missing
        SQLAlchemyError: For database connection/operation errors
        ValueError: If DataFrame is provided but invalid

    Example:
        >>> engine, success = execute_database_pipeline(happiness_df)
        >>> if success:
        ...     print("Database operations completed successfully")
    """
    try:
        # Step 1: Create database engine
        engine = create_db_engine(config_path)
        if engine is None:
            print("❌ Failed to create database engine")
            return None, False

        # Step 2: Create table structure
        try:
            create_table(engine)
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            return engine, False

        # Step 3: Insert data if DataFrame provided
        if df is not None:
            if not isinstance(df, pd.DataFrame) or df.empty:
                raise ValueError("Provided data must be a non-empty pandas DataFrame")
            
            try:
                insert_data(df, engine)
                print("✅ Data insertion completed successfully")
            except Exception as e:
                print(f"❌ Data insertion failed: {e}")
                return engine, False

        return engine, True

    except Exception as e:
        print(f"❌ Database pipeline failed: {e}")
        return None, False