import argparse
from getpass import getpass
import psycopg
import requests
from requests.auth import HTTPBasicAuth
import gzip
import json

API_USER = ""
API_PASSWORD = ""

# API Endpoint
API_BASE_URL = "https://publicdatafeeds.networkrail.co.uk/ntrod"
API_CORPUS = "/SupportingFileAuthenticate?type=CORPUS"
API_SMART = "/SupportingFileAuthenticate?type=SMART"


def str_fmt(in_str):
    return None if in_str.isspace() or len(in_str) == 0 else in_str.strip()


def int_fmt(in_int):
    try:
        return int(in_int)
    except:
        return None


def stanox_fmt(in_int):
    try:
        # 0 is to be handled as null value
        return None if in_int == "0" else int(in_int)
    except:
        return None

# Schema
# SMART: Berth Stepping Data
# BERTHDATA


class SmartRecord:
    def __init__(self, data):
        self.td = str_fmt(data["TD"])
        self.from_berth = str_fmt(data["FROMBERTH"])
        self.to_berth = str_fmt(data["TOBERTH"])
        self.from_line = str_fmt(data["FROMLINE"])
        self.to_line = str_fmt(data["TOLINE"])
        self.berth_offset = int_fmt(data["BERTHOFFSET"])
        self.platform = str_fmt(data["PLATFORM"])
        self.event = str_fmt(data["EVENT"])
        self.route = str_fmt(data["ROUTE"])
        self.stanox = stanox_fmt(data["STANOX"])
        self.stanme = str_fmt(data["STANME"])
        self.step_type = str_fmt(data["STEPTYPE"])
        self.comment = str_fmt(data["COMMENT"])


# CORPUS: Location Reference Data
# TIPLOCDATA
class CorpusRecord:
    def __init__(self, data):
        self.stanox = stanox_fmt(data["STANOX"])
        self.uic_code = str_fmt(data["UIC"])
        self.location_code = str_fmt(data["3ALPHA"])
        self.tiploc_code = str_fmt(data["TIPLOC"])
        self.nlc = int_fmt(data["NLC"])
        self.nlc_description = str_fmt(data["NLCDESC"])
        self.description = str_fmt(data["NLCDESC16"])

# Database functions


def truncate_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE nrod.reference_smart;")
        cursor.execute("TRUNCATE nrod.reference_corpus;")


def insert_smart_record(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO nrod.reference_smart VALUES (
                %(td)s,
                %(from_berth)s,
                %(to_berth)s,
                %(from_line)s,
                %(to_line)s,
                %(berth_offset)s,
                %(platform)s,
                %(event)s,
                %(route)s,
                %(stanox)s,
                %(stanme)s,
                %(step_type)s,
                %(comment)s
            );
        """,
            data,
        )


def insert_corpus_record(connection, data):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO nrod.reference_corpus VALUES (
                %(stanox)s,
                %(uic_code)s,
                %(location_code)s,
                %(tiploc_code)s,
                %(nlc)s,
                %(nlc_description)s,
                %(description)s
            );
        """,
            data,
        )


# Updater


def update_metadata(connection):
    # Responses are gzip compressed files
    custom_header = {'Accept-Encoding': 'gzip'}
    res_smart = requests.get(API_BASE_URL + API_SMART, headers=custom_header,
                             auth=HTTPBasicAuth(API_USER, API_PASSWORD))
    smartdata = str(gzip.decompress(res_smart.content), 'utf-8')
    smart = json.loads(smartdata)
    for k in smart["BERTHDATA"]:
        data = SmartRecord(k)
        insert_smart_record(connection, vars(data))
    res_corpus = requests.get(
        API_BASE_URL + API_CORPUS, headers=custom_header, auth=HTTPBasicAuth(API_USER, API_PASSWORD))
    corpusdata = str(gzip.decompress(res_corpus.content), 'utf-8')
    corpus = json.loads(corpusdata)
    for k in corpus["TIPLOCDATA"]:
        data = CorpusRecord(k)
        insert_corpus_record(connection, vars(data))


def main():
    ap = argparse.ArgumentParser(
        prog="rata-metadata", description="Metadata Import Tool", conflict_handler="resolve")

    # Postgres related arguments
    ap.add_argument("-d", "--dbname", required=True,
                    help="specifies the name of the database to connect to")
    ap.add_argument(
        "-h",
        "--host",
        default="localhost",
        required=False,
        help="specifies the host name on which the server is running",
    )
    ap.add_argument(
        "-p", "--port", default=5432, required=False, help="specifies the port on which the server is listening"
    )
    ap.add_argument("-U", "--username", required=True,
                    help="connect to the database as the username")
    ap.add_argument(
        "-W",
        "--password",
        required=False,
        action="store_true",
        help="prompt for a password before connecting to a database",
    )
    ap.add_argument("-t", required=False, action="store_true",
                    help="test only without committing")
    args = ap.parse_args()

    # Postgres connection details
    dsn = f"dbname={args.dbname} user={args.username} host={args.host} port={args.port}"

    # Prompt for a password if requested
    if args.password == True:
        args.password = getpass(prompt="Password: ", stream=None)
        dsn = f"{dsn} password={args.password}"

    # Create a connection
    with psycopg.connect(dsn) as connection:
        truncate_tables(connection)
        update_metadata(connection)

        # If a test then rollback otherwise commit
        if args.t == True:
            connection.rollback()
        else:
            connection.commit()


if __name__ == "__main__":
    main()
