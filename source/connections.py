import sys
import boto3
import os
import psycopg2

endpoint = (
    "aurora-postgresql-us-east-1.cluster-cqcdehswzz9l.us-east-1.rds.amazonaws.com"
)
ENDPOINT = (
    "aurora-postgresql-us-east-1.cluster-cqcdehswzz9l.us-east-1.rds.amazonaws.com"
)
PORT = "5432"
USR = "master"
REGION = "us-east-1"
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

# gets the credentials from .aws/credentials
session = boto3.Session(profile_name="default")
client = session.client("rds")

token = client.generate_db_auth_token(
    DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION
)

try:
    conn = psycopg2.connect(
        "dbname=testdb user=master host=aurora-postgresql-us-east-1.cluster-cqcdehswzz9l.us-east-1.rds.amazonaws.com password=Arcsight?123 port=5432"
    )
    cur = conn.cursor()
    cur.execute(
        """SELECT column_name, is_nullable , data_type, character_maximum_length FROM  information_schema.columns WHERE table_name = 'access_request_overview_igasaas_m'"""
    )
    query_results = cur.fetchall()
except Exception as e:
    print("Database connection failed due to {}".format(e))
