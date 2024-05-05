mysql_host = '127.0.0.1'  # Hostname or IP address of your MySQL server
mysql_user = 'root'        # Username for MySQL server
mysql_password = 'password'  # Password for MySQL server (replace 'password' with your actual password)
mysql_port = '3306'        # Port for MySQL server
mysql_database = 'Ali'     # Database name

# Connect to MySQL server
connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    port=mysql_port,
    database=mysql_database
)

