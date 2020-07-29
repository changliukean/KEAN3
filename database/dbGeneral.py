import mysql.connector


HOST='kindledb.cfdmlfy5ocmf.us-west-2.rds.amazonaws.com'
USER='Andrew'
PASSWORD='Kindle01'
DATABASE='kean3'
PROD_DATABASE = 'kean'


def config_connection(host, user, password, database):
    conn_ins = mysql.connector.connect(host=host, user=user, password=password, database=database)
    return conn_ins













# #
