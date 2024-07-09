from src.functions import *

if __name__ == '__main__':
    setup_logger()
    aws_cur = connect_to_aws()
    database_list = get_aws_database_names(aws_cur)
    data_to_add = get_aws_daily_revenue(aws_cur, database_list, days=7)
    
    postgres_engine = connect_to_postgres()
    write_aws_data_to_postgres(data_to_add, postgres_engine)
    