from connection import conn

cursor = conn.cursor()

# Creating raw, staging and master tables
raw_table = '''
    CREATE TABLE raw_table(
    _id VARCHAR(255),
    IOS_App_Id int,
    Title VARCHAR(255),
    Developer_Name VARCHAR(255),
    Developer_IOS_Id FLOAT,
    IOS_Store_Url VARCHAR(255),
    Seller_Official_Website VARCHAR(255),
    Age_Rating VARCHAR(255),
    Total_Average_Rating FLOAT,
    Total_Number_of_Ratings FLOAT,
    Average_Rating_For_Version FLOAT,
    Number_of_Ratings_For_Version INT,
    Original_Release_Date VARCHAR(255),
    Current_Version_Release_Date VARCHAR(255),
    Price_USD FLOAT,
    Primary_Genre VARCHAR(255),
    All_Genres VARCHAR(255),
    Languages VARCHAR(255),
    Description VARCHAR(5000)
    )
'''


stage_table= '''
    CREATE TABLE stage_table(
    _id VARCHAR(255),
    IOS_App_Id int,
    Title VARCHAR(255),
    Developer_Name VARCHAR(255),
    Developer_IOS_Id FLOAT,
    IOS_Store_Url VARCHAR(255),
    Seller_Official_Website VARCHAR(255),
    Age_Rating VARCHAR(255),
    Total_Average_Rating FLOAT,
    Total_Number_of_Ratings FLOAT,
    Average_Rating_For_Version FLOAT,
    Number_of_Ratings_For_Version INT,
    Original_Release_Date VARCHAR(255),
    Current_Version_Release_Date VARCHAR(255),
    Price_USD FLOAT,
    Primary_Genre VARCHAR(255),
    All_Genres VARCHAR(255),
    Languages VARCHAR(255),
    Description VARCHAR(5000)
    )
'''


master_table= '''
    CREATE TABLE master_table(
    _id VARCHAR(255),
    IOS_App_Id int,
    Title VARCHAR(255),
    Developer_Name VARCHAR(255),
    Developer_IOS_Id FLOAT,
    IOS_Store_Url VARCHAR(255),
    Seller_Official_Website VARCHAR(255),
    Age_Rating VARCHAR(255),
    Total_Average_Rating FLOAT,
    Total_Number_of_Ratings FLOAT,
    Average_Rating_For_Version FLOAT,
    Number_of_Ratings_For_Version INT,
    Original_Release_Date VARCHAR(255),
    Current_Version_Release_Date VARCHAR(255),
    Price_USD FLOAT,
    Primary_Genre VARCHAR(255),
    All_Genres VARCHAR(255),
    Languages VARCHAR(255),
    Description VARCHAR(5000)
    )
'''

# Execute tables creation statement
cursor.execute(raw_table)
cursor.execute(stage_table)
cursor.execute(master_table)

conn.commit()

# Define the pipe (stream) configuration
pipe_name = 'raw_stream'
table_name = 'stage_table'
stage_name = 'first_stage'

raw_stream = f'''
CREATE PIPE {pipe_name}
    AS
    COPY INTO {table_name}
    FROM @{stage_name}
    FILE_FORMAT = (FORMAT_NAME = 'my_file_format')
'''

# Execute the pipe creation statement
cursor.execute(raw_stream)

# Define the pipe (stream) configuration
pipe_name = 'stage_stream'
table_name = 'master_table'
stage_name = 'second_stage'

stage_stream = f'''
CREATE PIPE {pipe_name}
    AS
    COPY INTO {table_name}
    FROM @{stage_name}
    FILE_FORMAT = (FORMAT_NAME = 'my_file_format')
'''


cursor.execute(raw_table)
cursor.execute(stage_table)
cursor.execute(master_table)
cursor.execute(raw_stream)
cursor.execute(stage_stream)

conn.commit()