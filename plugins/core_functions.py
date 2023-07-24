from connection import conn
import pandas as pd

def read_csv_to_raw_table():
    # Read data from the CSV file and insert into the RAW_TABLE.
    csv_file_path = r'/data/apps_info.csv'
    df = pd.read_csv(csv_file_path)

    # Read data from the CSV file
    raw_table_name = 'RAW_TABLE'

    # Create a cursor to execute SQL queries.
    cursor = conn.cursor()

    # Convert the DataFrame to a CSV string.
    csv_string = df.to_csv(index=False)

    sql=f"""
    COPY INTO {raw_table_name} 
    FROM (SELECT '{csv_string}') 
    FILE_FORMAT = (TYPE = 'CSV')
    """
    # Execute the SQL statement.
    cursor.execute(sql)
    # Close the cursor and connection.
    cursor.close()
    conn.close()

def raw_to_stage_table():
    # Define table naming
    stage_table_name = 'STAGE_TABLE'
    raw_stream_name ='RAW_STREAM'
    # Create a cursor to execute SQL queries.
    cursor = conn.cursor()
    sql_query=f"""
    INSERT INTO {stage_table_name} (
    _id,
    IOS_App_Id,
    Title,
    Developer_Name,
    Developer_IOS_Id,
    IOS_Store_Url,
    Seller_Official_Website,
    Age_Rating,
    Total_Average_Rating,
    Total_Number_of_Ratings,
    Average_Rating_For_Version,
    Number_of_Ratings_For_Version,
    Original_Release_Date,
    Current_Version_Release_Date,
    Price_USD,
    Primary_Genre,
    All_Genres,
    Languages,
    Description) SELECT _id,
    IOS_App_Id,
    Title,
    Developer_Name,
    Developer_IOS_Id,
    IOS_Store_Url,
    Seller_Official_Website,
    Age_Rating,
    Total_Average_Rating,
    Total_Number_of_Ratings,
    Average_Rating_For_Version,
    Number_of_Ratings_For_Version,
    Original_Release_Date,
    Current_Version_Release_Date,
    Price_USD,
    Primary_Genre,
    All_Genres,
    Languages,
    Description
    FROM {raw_stream_name}
    """
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit and close the connection
    conn.commit()
    conn.close()

def stage_to_master_table():
    # Define table naming
    master_table_name = 'MASTER_TABLE'
    stage_stream_name ='STAGE_STREAM'
    # Create a cursor to execute SQL queries.
    cursor = conn.cursor()
    sql_query=f"""
    INSERT INTO {master_table_name} (
    _id,
    IOS_App_Id,
    Title,
    Developer_Name,
    Developer_IOS_Id,
    IOS_Store_Url,
    Seller_Official_Website,
    Age_Rating,
    Total_Average_Rating,
    Total_Number_of_Ratings,
    Average_Rating_For_Version,
    Number_of_Ratings_For_Version,
    Original_Release_Date,
    Current_Version_Release_Date,
    Price_USD,
    Primary_Genre,
    All_Genres,
    Languages,
    Description) SELECT _id,
    IOS_App_Id,
    Title,
    Developer_Name,
    Developer_IOS_Id,
    IOS_Store_Url,
    Seller_Official_Website,
    Age_Rating,
    Total_Average_Rating,
    Total_Number_of_Ratings,
    Average_Rating_For_Version,
    Number_of_Ratings_For_Version,
    Original_Release_Date,
    Current_Version_Release_Date,
    Price_USD,
    Primary_Genre,
    All_Genres,
    Languages,
    Description
    FROM {stage_stream_name}
    """
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit and close the connection
    conn.commit()
    conn.close()