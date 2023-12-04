import azure.functions as func
import logging
import os
import streamlit as st
import app_utils
app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="spie-uc/{name}",
                               connection="") 
def uc_trigger_spie(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    file_extension = os.path.splitext(myblob.name)[1].lower()
    logging.info(f"This is your file Type : {file_extension}")

    file_content = myblob.read().decode('utf-8')
    logging.info(f"This is your file content: {file_content}")

    session = app_utils.get_snowpark_session()
    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show()

    app_utils.process_file(session,file_extension,file_content)