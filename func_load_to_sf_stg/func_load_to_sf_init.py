import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import tempfile
import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import math

file_size = 1
def main(blob: func.InputStream):
    
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {blob.name}\n")
                 #f"Blob Size: {file_size/ 1024} kilobytes")
    
    file_name = os.path.basename(blob.name)

    if file_name.endswith('.csv'):
        process_csv3(blob, file_name)
    elif file_name.endswith('.parquet'):
        process_parquet(blob, file_name)
    else:
        logging.info(f"Ignoring file: {file_name}")

#'/Users/deen/Documents/Work/Repos/Public/Utilities/Parquet_Files/2023-13_10-38/order_items.parquet'
def process_csv(blob, file_name):
    output_container_name = "snowflake-csv-stage"
    today_date = datetime.today().strftime("%Y/%m/%d")
    file_name_without_extension = os.path.splitext(file_name)[0]
    if file_size > 100 * 1024:  # Check if file size is greater than 100KB
        chunk_size = 200 * 1024 # 1 KB

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Save the InputStream to a temporary file
            temp_file.write(blob.read())
            temp_file.close()
            num_chunks = (file_size // chunk_size) + 1
            logging.info(f"Temporary file size is {os.path.getsize(temp_file.name)} bytes")
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(temp_file.name, chunksize=chunk_size)
            logging.info(f"object df is of type {type(df)}")
            chunk_index = 1
            for chunk in df:
                # Save the chunk in the snowflake-csv-stage container
                logging.info(f"object chunk is of type {type(chunk)}\n")
                chunk_name = f"{today_date}/{file_name_without_extension}_{chunk_index}.csv"  # Use the extracted file name
                save_to_snowflake_stage(chunk_name, chunk.to_csv(index=False), output_container_name)

                chunk_index += 1

            os.unlink(temp_file.name)  # Delete the temporary file

    else:
        logging.info(f"CSV file size is less than or equal to 10KB. No splitting required.")
        output_container_name = "snowflake-csv-stage"
        save_to_snowflake_stage(file_name, blob.read(), output_container_name)
    

def process_csv2(blob, file_name):
    #Set the output container
    output_container_name = "snowflake-csv-stage"
    #Set today's date for proper partitioning by date
    today_date = datetime.today().strftime("%Y/%m/%d")

    file_name_without_extension = os.path.splitext(file_name)[0]
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(blob.read())
    temp_file.close()
    file_size = os.path.getsize(temp_file.name)
    chunk_size = int(os.environ["ChunkSize"]) 
    logging.info(f"filesize is: {file_size}\n"
                 f"Chunk Size is: {chunk_size}")
    
    if file_size > chunk_size:  # Check if file size is greater than the chunk_size

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(temp_file.name)

        # Calculate the approximate number of chunks based on the chunk size
        num_chunks = (file_size // chunk_size) + 1

        # Calculate the number of rows per chunk
        rows_per_chunk = len(df) // num_chunks

        # Split the DataFrame into chunks
        chunks = [df[i*rows_per_chunk:(i+1)*rows_per_chunk] for i in range(num_chunks)]

        # Save each chunk as a separate CSV file
        for i, chunk in enumerate(chunks):
            chunk_name = f"{today_date}/{file_name_without_extension}_{i+1}.csv"
            save_to_snowflake_stage(chunk_name, chunk.to_csv(index=False), output_container_name)

        os.unlink(temp_file.name)  # Delete the temporary file 

    else:
        logging.info(f"CSV file size is less than or equal to 10KB. No splitting required.")
        save_to_snowflake_stage(file_name, blob.read(), output_container_name)


def process_csv3(blob, file_name):
    #Set the output container
    output_container_name = "snowflake-csv-stage"
    #Set today's date for proper partitioning by date
    today_date = datetime.today().strftime("%Y/%m/%d")

    file_name_without_extension = os.path.splitext(file_name)[0]
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(blob.read())
    temp_file.close()
    file_size = os.path.getsize(temp_file.name)
    #chunk_size = 5 * 1024 * 1024
    chunk_size = int(os.environ["ChunkSize"]) 
    #chunk_size = int(os.environ.get('ChunkSize'))

    logging.info(f"File size is {file_size/1024} kilobytes\n"
                 f"Chunk size is {chunk_size/1024} kilobytes\n"
                 f"Datatype is {type(chunk_size)}")
    # Calculate the approximate number of chunks based on the chunk size
    num_chunks = (file_size // chunk_size) + 1

    if file_size > chunk_size:  # Check if file size is greater than the chunk_size

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(temp_file.name)

        # Calculate the approximate number of chunks based on the chunk size
        num_chunks = (file_size // chunk_size) + 1

        # Calculate the number of rows per chunk
        target_rows_per_chunk = len(df) // num_chunks

        # Initialize variables
        chunks = []
        current_chunk = pd.DataFrame(columns=df.columns)
        current_chunk_size = 0

        # Iterate over each row in the DataFrame
        for _, row in df.iterrows():
            # Calculate the current row size
            row_size = row.memory_usage(deep=True)
            logging.info(f"Calculating the current row size: {row_size}")

            # Check if adding the current row exceeds the chunk size
            if current_chunk_size + row_size > chunk_size:
                # Add the current chunk to the list of chunks
                logging.info("Adding the current chunk to the list of chunks")
                chunks.append(current_chunk.copy())

                # Reset the current chunk
                current_chunk = pd.DataFrame(columns=df.columns)
                current_chunk_size = 0

            # Add the current row to the current chunk
            #current_chunk = current_chunk.append(row)
            current_chunk = pd.concat([current_chunk, pd.DataFrame([row], columns=df.columns)], ignore_index=True)
            current_chunk_size += row_size

        # Add the last chunk to the list of chunks
        if not current_chunk.empty:
            chunks.append(current_chunk.copy())

        # Save each chunk as a separate CSV file
        for i, chunk in enumerate(chunks):
            chunk_name = f"{today_date}/{file_name_without_extension}_{i+1}.csv"
            save_to_snowflake_stage(chunk_name, chunk.to_csv(index=False), output_container_name)

    else:
        logging.info(f"CSV file size is less than or equal to {chunk_size/1024}KB. No splitting required.")
        save_to_snowflake_stage("{today_date}/{file_name}", blob.read(), output_container_name)


def process_parquet(blob, file_name):
    #Set the output container
    output_container_name = "snowflake-parquet-stage"
    #Set today's date for proper partitioning by date
    today_date = datetime.today().strftime("%Y/%m/%d")

    file_name_without_extension = os.path.splitext(file_name)[0]
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(blob.read())
    temp_file.close()
    file_size = os.path.getsize(temp_file.name)
    chunk_size = int(os.environ["ChunkSize"]) #500 * 1024
    # Calculate the approximate number of chunks based on the chunk size
    num_chunks = (file_size // chunk_size) + 1

    logging.info(f"File size is {file_size/1024} kilobytes\n"
                 f"Chunk size is {chunk_size/1024} kilobytes\n"
                 f"chunk Datatype is {type(chunk_size)}"
                 f"Approximate number of chunks expected is {num_chunks}")
    
    

    if file_size > chunk_size:  # Check if file size is greater than the chunk_size

        # Read the CSV file into a pandas DataFrame
        table = pq.read_table(temp_file.name)
        df = table.to_pandas()

        # Calculate the approximate number of chunks based on the chunk size
        num_chunks = (file_size // chunk_size) + 1

        # Calculate the number of rows per chunk
        target_rows_per_chunk = len(df) // num_chunks


        # Initialize variables
        chunks = []
        current_chunk = pd.DataFrame(columns=df.columns)
        current_chunk_size = 0

        # Iterate over each row in the DataFrame
        for _, row in df.iterrows():
            # Calculate the current row size
            row_size = row.memory_usage(deep=True)
           # logging.info(f"Calculating the current row size: {row_size}")

            # Check if adding the current row exceeds the chunk size
            if current_chunk_size + row_size > chunk_size:
                # Add the current chunk to the list of chunks
               # logging.info("Adding the current chunk to the list of chunks")
                chunks.append(current_chunk.copy())

                # Reset the current chunk
                current_chunk = pd.DataFrame(columns=df.columns)
                current_chunk_size = 0

            # Add the current row to the current chunk
            #current_chunk = current_chunk.append(row)
            current_chunk = pd.concat([current_chunk, pd.DataFrame([row], columns=df.columns)], ignore_index=True)
            current_chunk_size += row_size

        # Add the last chunk to the list of chunks
        if not current_chunk.empty:
            chunks.append(current_chunk.copy())

        #for i, chunk in enumerate(chunks):
        #    chunk_name = f"{today_date}/{file_name_without_extension}_{i+1}.csv"
        #    save_to_snowflake_stage(chunk_name, chunk.to_csv(index=False), output_container_name)

        # Save each chunk as a separate CSV file
        for i, chunk in enumerate(chunks):
            chunk_filename = f"{today_date}/{file_name_without_extension}_{i+1}.parquet"
            with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
                #chunk.to_parquet(temp_file.name)
                table = pa.Table.from_pandas(chunk) #
                pq.write_table(table, temp_file.name,version='1.0') #
                with open(temp_file.name, "rb") as data:
                    save_to_snowflake_stage(chunk_filename, data, output_container_name)


    else:
        logging.info(f"Parquet file size is less than or equal to {chunk_size/1024}KB. No splitting required.")
        save_to_snowflake_stage(f"{today_date}/{file_name}", blob.read(), output_container_name)

def process_parquet2(blob, file_name):
    #file_size = os.stat(file_name).st_size
    #file_size = len(blob.read())
    logging.info(f"File size is {file_size}")
    output_container_name = "snowflake-parquet-stage"

    if file_size > 10 * 1024:  # Check if file size is greater than 100KB
        chunk_size = 10 * 1024  # 100 KB

        with blob.open() as file:
            index = 0
            chunk_index = 1
            while index < file_size:
                # Set the file position to the current index
                file.seek(index)

                # Read the chunk
                chunk = file.read(chunk_size)

                # Save the chunk in the snowflake-parquet-stage container
                chunk_name = f"{blob.name}_{chunk_index}.parquet"
                save_to_snowflake_stage(chunk_name, chunk, output_container_name)

                # Update the index for the next chunk
                index += chunk_size
                chunk_index += 1

    else:
        logging.info(f"Parquet file size is less than or equal to 100KB. No splitting required.")


def save_to_snowflake_stage(file_name, data, container_name):
    # Code to save the data in the appropriate snowflake-stage container
    # Replace with your own implementation
    container = container_name # Replace with your output container name
    connection_string = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    output_container_client = blob_service_client.get_container_client(container)
    logging.info(f"Saving file {file_name} to {container_name}.\n"
                 f"Data is of type {type(data)}")
    output_container_client.upload_blob(name=file_name, data=data, overwrite=True)


def save_to_snowflake_parquet_stage(file_name, data):
    # Code to save the data in the snowflake-parquet-stage container
    # Replace with your own implementation
    logging.info(f"Saving file {file_name} to snowflake-parquet-stage.")
