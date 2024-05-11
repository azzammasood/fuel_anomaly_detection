import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, MetaData, Table, text, insert
import logging

def read_sql_table(table_name, db_connection):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, db_connection.engine)
    return df

def add_new_columns(df, table_name, db_connection):
    engine = db_connection.engine
    connection = engine.connect()
    inspector = inspect(engine)
    
    existing_columns = inspector.get_columns(table_name)
    existing_column_names = [col['name'] for col in existing_columns]

    new_columns = [col for col in df.columns if col not in existing_column_names]

    if new_columns:
        for col in new_columns:
            col_type = str(df[col].dtype)
            if 'int' in col_type:
                col_type = 'INTEGER'
            elif 'float' in col_type:
                col_type = 'FLOAT'
            elif 'datetime' in col_type:
                col_type = 'TIMESTAMP'
            else:
                col_type = 'VARCHAR'
            
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"))
        logging.info(f"New columns {new_columns} added to table '{table_name}'.")

    connection.close()

def create_table_if_not_exists(df, table_name, db_connection):
    engine = db_connection.engine
    inspector = inspect(engine)

    if not inspector.has_table(table_name):
        df.head(0).to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Table '{table_name}' created successfully.")

def insert_data_to_table(df, table_name, db_connection):
    create_table_if_not_exists(df, table_name, db_connection)
    
    engine = db_connection.engine
    metadata = MetaData()
    metadata.bind = engine
    table = Table(table_name, metadata, autoload_with=engine)

    add_new_columns(df, table_name, db_connection)

    records = df.to_dict(orient='records')

    with engine.begin() as conn:
        for record in records:
            stmt = table.insert().values(record)
            try:
                conn.execute(stmt)
            except SQLAlchemyError as e:
                logging.error(f"Error occurred during data insert to '{table_name}': {e}")
                continue

def update_data_in_table(df, table_name, db_connection, unique_columns=['siteid', 'updatetime']):
    engine = db_connection.engine
    metadata = MetaData()
    metadata.bind = engine
    table = Table(table_name, metadata, autoload_with=engine)

    records = df.to_dict(orient='records')

    with engine.begin() as conn:
        for record in records:
            conditions = [table.c[col] == record[col] for col in unique_columns]
            stmt = table.update().where(*conditions).values(record)
            try:
                conn.execute(stmt)
            except SQLAlchemyError as e:
                logging.error(f"Error occurred during data update to '{table_name}': {e}")
                continue

def truncate_table(table_name, db_connection):
    engine = db_connection.engine
    inspector = inspect(engine)
    
    if inspector.has_table(table_name):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            logging.info(f"Table '{table_name}' has been truncated successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Error truncating table '{table_name}': {e}")
    else:
        logging.warning(f"Table '{table_name}' does not exist and cannot be truncated.")

def remove_siteid_from_table(siteid, table_name, db_connection):
    engine = db_connection.engine
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table_name} WHERE siteid = :siteid"), {'siteid': siteid})
        logging.info(f"Entries for siteid '{siteid}' have been removed from table '{table_name}'.")
    except SQLAlchemyError as e:
        logging.error(f"Error removing siteid '{siteid}' from table '{table_name}': {e}")

def manage_site_wise_alarm(df, site_wise_alarm, current_time, db_connection):
    engine = db_connection.engine
    metadata = MetaData()
    metadata.bind = engine
    table = Table(site_wise_alarm, metadata, autoload_with=engine)
    
    df['inserted_at'] = current_time

    add_new_columns(df, site_wise_alarm, db_connection)

    records = df.to_dict(orient='records')

    with engine.begin() as conn:
        for record in records:
            stmt = insert(table).values(record)
            try:
                # Update on conflict
                update_stmt = stmt.on_conflict_do_update(
                    index_elements=['siteid'],
                    set_=record
                )
                conn.execute(update_stmt)
            except SQLAlchemyError as e:
                logging.error(f"Error occurred during site-wise alarm update for '{site_wise_alarm}': {e}")
                continue
