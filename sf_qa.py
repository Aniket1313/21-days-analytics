#This piece of code is used to send SF Query output to Python 

import snowflake.connector
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Snowflake connection parameters
snowflake_user = 'your_snowflake_user'
snowflake_password = 'your_snowflake_password'
snowflake_account = 'your_snowflake_account'
snowflake_warehouse = 'your_snowflake_warehouse'
snowflake_database = 'your_snowflake_database'
snowflake_schema = 'your_snowflake_schema'

# Email configuration
sender_email = 'your_email@gmail.com'
receiver_email = 'recipient_email@gmail.com'
email_subject = 'Snowflake Query Results'

# Snowflake queries with corresponding table names
queries = [
    {
        'name': 'Table 1 - V_DIGITAL_JOINED_RS',
        'query': """
            SELECT 
                SUM(IMPRESSIONS) AS IMPRESSIONS,
                SUM(CLICKS) AS CLICKS,
                SUM(COST_PLUS_FEE) AS SPEND
            FROM 
                GR_KINESSO.MH_MORGANSTANLEY_US.TBL_V_DIGITAL_JOINED_RS
            WHERE 
                DATE > '2023-01-01'
        """
    },
    {
        'name': 'Table 2 - Another_Table',
        'query': """
            SELECT 
                *
            FROM 
                your_another_table
            WHERE 
                condition
        """
    },
    # Add more queries as needed
]

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)

# Execute queries and fetch results
results_list = []
cursor = conn.cursor()
for query_info in queries:
    cursor.execute(query_info['query'])
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    df_rounded = df.round(2)  # Round off to 2 decimals
    results_list.append({'name': query_info['name'], 'data': df_rounded})

# Close Snowflake connection
cursor.close()
conn.close()

# Send email with tabular data
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = email_subject

# Concatenate the HTML tables with table names in MIMEText with <font> tag
html_tables = ''
for result_info in results_list:
    html_tables += f"<p><b>{result_info['name']}</b></p><font face='Arial'>{result_info['data'].to_html(index=False)}</font><br><br>"

msg.attach(MIMEText(html_tables, 'html'))

# Connect to SMTP server and send email
with smtplib.SMTP('smtp.gmail.com', 587) as server:
    server.starttls()
    server.login(sender_email, 'your_email_password')
    server.sendmail(sender_email, receiver_email, msg.as_string())
