from snowflake import snowpark 
import streamlit as st
import pandas as pd
import io
import logging
import json


def get_snowpark_session() -> snowpark.Session:
    connection_parameters = {
            "ACCOUNT":st.secrets['snowflake']['account'],
            "USER":st.secrets['snowflake']['user'],
            "PASSWORD":st.secrets['snowflake']['password'], 
            "ROLE":"DE_ROLE",
            "DATABASE":"bs_demo_db",
            "SCHEMA":"trigger_sch",
            "WAREHOUSE":"bs_demo_wh"
        }
    # creating snowflake session object
    return snowpark.Session.builder.configs(connection_parameters).create()


# Function to create table and ingest data based on file extension
def process_file(session: snowpark.Session,file_extension, file_content):
    table_name = f'table_{file_extension[1:]}'

    if file_extension == ".csv":
        try:
            sp_df = session.createDataFrame(pd.read_csv(io.StringIO(file_content)))
            sp_df.write.mode('overwrite').save_as_table(table_name)
        except Exception as e:
            logging.error(f"ERROR ahead CSV: {e}")

    if file_extension == ".json":
        try:
            
            #data = json.loads(file_content)
            sp_df = session\
            .createDataFrame(pd.DataFrame(pd.read_json(io.StringIO(file_content), typ='series')))
            sp_df.write.mode('overwrite').save_as_table(table_name)
        except Exception as e:
            logging.error(f"ERROR ahead Json : {e}")
