import pymysql
from dotenv import load_dotenv
import os
from src.models import Daily_Revenue
from sqlalchemy import create_engine, select, delete, and_
from sqlalchemy.orm import Session
from decimal import Decimal
from loguru import logger
from datetime import datetime

def setup_logger():
    os.makedirs("logs", exist_ok=True)
    
    date = datetime.now().strftime("%Y-%m-%d")
    
    logger.add(f'logs/{date}.log', rotation='1 day', retention='7 days', level='INFO')

def connect_to_aws():
    load_dotenv()

    aws_host = os.getenv("AWS_HOSTNAME")
    aws_user = os.getenv("AWS_USERNAME")
    aws_password = os.getenv("AWS_PASSWORD")
    aws_port = int(os.getenv("AWS_PORT"))

    try:
        conn = pymysql.connect(
            host=aws_host,
            port=aws_port,
            user=aws_user,
            password=aws_password,
        )
        logger.success("Connected to AWS")
        return conn.cursor()
    except Exception as e:
        logger.error(f"Error connecting to AWS: {e}")


def get_aws_database_names(aws_cur) -> list:
    try:
        aws_cur.execute("SHOW DATABASES")
        databases = aws_cur.fetchall()
        databases = [db for (db,) in databases if "ezymerged" in db and 'history' not in db]
        logger.success("Fetched database names")
        return databases
    except Exception as e:
        logger.error(f"Error fetching database names: {e}")

def get_aws_daily_revenue(aws_cur, database_list: list, days = 7) -> list:
    hospital_mapping = {
        "ezymerged_ahow":"Walnut",
        "ezymerged_carlsbadvah":"The Village Carlsbad",
        "ezymerged_danapointah":"Dana Point",
        "ezymerged_delmarvh":"Del Mar",
        "ezymerged_middletownvet":"Middletown",
        "ezymerged_murphyave":"Murphy",
        "ezymerged_mvpc":"Mill Valley VH LP",
        "ezymerged_nichols":"Nichols",
        "ezymerged_noe":"Noe Valley",
        "ezymerged_paccoastah":"Pacific Coast",
        "ezymerged_sfamc":"AMC",
        "ezymerged_sfpethospital":"SF Pet",
        "ezymerged_shoreline":"Shoreline",
        "ezymerged_talegaah":"Talega",
        "ezymerged_tcdaf":"Cat Doctor & Friends",
        "ezymerged_vegasvalley":"Vegas Valley",
        "ezymerged_curohah":"Heritage",
        "ezymerged_muirlands":"Muirlands",
        "ezymerged_whipple":"Whipple"
    }
    
    revenue_query = f"""
        SELECT
            date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00')) as "Date",
            SUM(invoicedata_amount) as "Revenue",
            COUNT(DISTINCT(invoice_uid)) as 'Invoice Count'
        FROM 
            invoice
        WHERE
            invoicedata_active = 1
            AND invoicedata_approvedat > 0
            AND date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00'))  >= current_date() - interval {days} day
        GROUP BY
            date(CONVERT_TZ(from_unixtime(invoicedata_duedate), '+00:00', '-08:00'))
    """
    
    data_to_add = []

    for database in database_list:
        try:
            aws_cur.execute(f"USE {database}")
            aws_cur.execute(revenue_query)
            result = aws_cur.fetchall()
            logger.success(f"Fetched data from {database}")
        except Exception as e:
            logger.error(f"Error fetching data from {database}: {e}")
            continue
        for row in result:
            date, total_gross_revenue, total_invoices = row
            # round total_gross_revenue to 2 decimal places
            total_gross_revenue = round(total_gross_revenue, 2)
            # total_gross_revenue = Decimal(total_gross_revenue)
            frmt_date = date.strftime("%Y-%m-%d")
            hospital = hospital_mapping[database]
            month = date.strftime("%b")
            # convert day into 3 letter day of the week
            day = date.strftime("%A")
            # get first 3 letters of the day
            day = day[:3]
            year = date.strftime("%Y")
            
            new_clients_query = f"""
                SELECT 
                    date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00')) as 'Created Date',
                    COUNT(DISTINCT(contact_uid)) as 'New Count'
                FROM
                    ezymerged_middletownvet.contact as contact
                WHERE
                    date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00')) = '{frmt_date}'
                GROUP BY
                    date(CONVERT_TZ(from_unixtime(contact_time),'+0:00','-8:00'))
                """
            try:
                aws_cur.execute(new_clients_query)
                new_clients = aws_cur.fetchall()
            except Exception as e:
                logger.error(f"Error fetching new clients from {database}: {e}")
    
            new_clients = new_clients[0][1] if new_clients else 0
            total_average_invoice = round(total_gross_revenue / total_invoices if total_invoices else 0,2)
            
            data_to_add.append(Daily_Revenue(
                hospital=hospital,
                date=date,
                year = year,
                month = month,
                day = day,
                total_avg_invoice = total_average_invoice,
                total_invoices = total_invoices,
                total_gross_revenue = total_gross_revenue,
                total_net_revenue = total_gross_revenue,
                total_new_clients = new_clients
            ))
    return data_to_add

def connect_to_postgres():
    load_dotenv()
    
    try:
        user = os.getenv("POSTGRES_USERNAME")
        password = os.getenv("POSTGRES_PASSWORD")
        hostname = os.getenv("POSTGRES_HOSTNAME")
        port = int(os.getenv("POSTGRES_PORT"))
        dbname = os.getenv("POSTGRES_DATABASE")

        # postgres connection string template
        engine= create_engine(f"postgresql://{user}:{password}@{hostname}:{port}/{dbname}")
        logger.success("Connected to Postgres")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to Postgres: {e}")

def write_aws_data_to_postgres(data_to_add: list, postgres_engine):
    with Session(postgres_engine) as session:
        added = 0
        errors = 0
        for data in data_to_add:
            stmt = select(Daily_Revenue).where(Daily_Revenue.hospital == data.hospital).where(Daily_Revenue.date == data.date)
            result = session.execute(stmt).first()
            if result:
                try:
                    session.delete(result[0])
                    session.commit()
                except Exception as e:
                    logger.error(f"Error deleting existing data: {e}")
            try:
                session.add(data)
                added += 1
                session.commit()
            except Exception as e:
                logger.error(f"Error adding data to Postgres: {e}")
                errors +=1
    logger.success(f"Added {added} records to Postgres with {errors} errors")