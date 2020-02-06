from ah_bank import balance_history
from ah_bank import transaction, bank_transaction
import ah_config
import ah_db
import arrow
import datetime
import logging
import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import text
import time


def getSQLdata(query, conn):
    '''Getting date from SQL server
    args:
    inputs:
           query (str): the query to SQL server
    output:
           df (pd.DataFrame): table in pandas dataframe format
    '''

    df = pd.read_sql(query, con=conn)
    return df


def get_df(db_name, sql):
    with ah_db.open_db_connection(db_name) as connection:
        return pd.read_sql(sql, con=connection)


def getESdata(user_id, predict=True):

    if predict:
        #   Get non-pending transactions for the last 6 months
        start_date = arrow.utcnow().replace(months=-6).format(transaction.ES_DATE_FORMAT)
        query_size = 10000
    else:
        start_date = '2010-01-01'
        query_size = 100000

    transactions = bank_transaction.getTransactions({
            'user_id': user_id,
            'pending': False,
            'start': start_date,
            'size': query_size
        })

    df = pd.DataFrame(transactions['items'])

    if 'ProviderTransactionId' in df.columns:
        if 'providerTransactionId' in df.columns:
            df['providerTransactionId'] = df['providerTransactionId'].fillna(df['ProviderTransactionId'])
        else:
            df['providerTransactionId'] = df['ProviderTransactionId']
        df = df.drop('ProviderTransactionId', axis=1)

    df.rename(columns={'createdOn': 'CreatedOn',
                       'amount': 'Amount',
                       'description': 'Description',
                       'postedDate': 'PostedDate',
                       'providerTransactionId': 'ProviderTransactionId',
                       'userId': 'UserId'},
              inplace=True)

    if df.shape[0] > 0:
        df = df[df['PostedDate'].notnull()]
        df['CreatedOn'] = df['CreatedOn'].map(lambda x: pd.to_datetime(x[:19]))
        df['PostedDate'] = df['PostedDate'].map(lambda x: pd.to_datetime(x[:10]))
    return df


def getESdatawithAccountId(user_id, bfp_account_ids, predict=True):
    if predict:
        #   Get non-pending transactions for the last 6 months
        start_date = arrow.utcnow().replace(months=-6).format(transaction.ES_DATE_FORMAT)
        query_size = 10000
    else:
        start_date = '2010-01-01'
        query_size = 100000

    if type(bfp_account_ids) != list:
        bfp_account_ids = [bfp_account_ids]

    transactions = bank_transaction.getTransactions({
        'user_id': user_id,
        'pending': False,
        'start': start_date,
        'bfp_account_id': bfp_account_ids,
        'size': query_size
    })

    df = pd.DataFrame(transactions['items'])

    if 'ProviderTransactionId' in df.columns:
        if 'providerTransactionId' in df.columns:
            df['providerTransactionId'] = df['providerTransactionId'].fillna(df['ProviderTransactionId'])
        else:
            df['providerTransactionId'] = df['ProviderTransactionId']
        df = df.drop('ProviderTransactionId', axis=1)

    df.rename(columns={'createdOn': 'CreatedOn',
                       'amount': 'Amount',
                       'description': 'Description',
                       'postedDate': 'PostedDate',
                       'providerTransactionId': 'ProviderTransactionId',
                       'userId': 'UserId'},
              inplace=True)

    if df.shape[0] > 0:
        df = df[df['PostedDate'].notnull()]
        df['CreatedOn'] = df['CreatedOn'].map(lambda x: pd.to_datetime(x[:19]))
        df['PostedDate'] = df['PostedDate'].map(lambda x: pd.to_datetime(x[:10]))
    return df


def getESdataForPayroll(user_id, predict=True):

    if predict:
        #   Get credit transactions for the last 6 months
        start_date = arrow.utcnow().replace(months=-6).format(transaction.ES_DATE_FORMAT)
        query_size = 10000
    else:
        start_date = '2010-01-01'
        query_size = 100000

    transactions = bank_transaction.getTransactions({
            'user_id': user_id,
            'amount_range': (0, 1000000),
            'start': start_date,
            'size': query_size
        })

    df = pd.DataFrame(transactions['items'])

    if 'ProviderTransactionId' in df.columns:
        if 'providerTransactionId' in df.columns:
            df['providerTransactionId'] = df['providerTransactionId'].fillna(df['ProviderTransactionId'])
        else:
            df['providerTransactionId'] = df['ProviderTransactionId']
        df = df.drop('ProviderTransactionId', axis=1)

    df.rename(columns={'createdOn': 'CreatedOn',
                       'amount': 'Amount',
                       'description': 'Description',
                       'providerTransactionId': 'ProviderTransactionId'},
              inplace=True)

    if df.shape[0] > 0:
        df['CreatedOn'] = df['CreatedOn'].map(lambda x: pd.to_datetime(x[:19]))

    if 'id' not in df.columns:
        df['id'] = -9

    return df


def getEStransaction(id):
    raise NotImplementedError("Not yet implemented")


def getESdataPending(user_id, predict=True):
    if predict:
        #   Get all transactions for the last 6 months
        start_date = arrow.utcnow().replace(months=-6).format(transaction.ES_DATE_FORMAT)
        query_size = 10000
    else:
        start_date = '2010-01-01'
        query_size = 100000

    transactions = bank_transaction.getTransactions({
            'user_id': user_id,
            'start': start_date,
            'size': query_size
        })

    df = pd.DataFrame(transactions['items'])

    if 'ProviderTransactionId' in df.columns:
        if 'providerTransactionId' in df.columns:
            df['providerTransactionId'] = df['providerTransactionId'].fillna(df['ProviderTransactionId'])
        else:
            df['providerTransactionId'] = df['ProviderTransactionId']
        df = df.drop('ProviderTransactionId', axis=1)

    df.rename(columns={'createdOn': 'CreatedOn',
                       'amount': 'Amount',
                       'description': 'Description',
                       'postedDate': 'PostedDate',
                       'providerTransactionId': 'ProviderTransactionId',
                       'userId': 'UserId'},
              inplace=True)

    if df.shape[0] > 0:
        if 'PostedDate' not in df.columns:
            df['PostedDate'] = df['CreatedOn']
        if 'Description' not in df.columns:
            df['Description'] = 'None'

        df = df[df['PostedDate'].notnull()]
        df['CreatedOn'] = df['CreatedOn'].map(lambda x: pd.to_datetime(x[:19]))
        df['PostedDate'] = df['PostedDate'].map(lambda x: pd.to_datetime(x[:10]))
    return df


def getESbankbalance(user_id, bfpProvidedBankAccountId, predict=True):
    if predict:
        start_date = arrow.utcnow().replace(months=-6).format('YYYY-MM-DDTHH:MM:SS')
        query_size = 10000
    else:
        start_date = '2010-01-01'
        query_size = 100000

    end_date = datetime.datetime.utcnow().isoformat()

    if type(bfpProvidedBankAccountId) != list:
        bfpProvidedBankAccountId = [bfpProvidedBankAccountId]

    history = balance_history.query({
            'user_id': user_id,
            'start': start_date,
            'end': end_date,
            'bfp_account_id': bfpProvidedBankAccountId,
            'size': query_size})

    df = pd.DataFrame(history)
    df = df.dropna(subset=['Balance'])

    if df.shape[0] == 0:
        return pd.DataFrame(columns=["BfpProvidedBankAccountId",
                                     "CreatedOn", "Balance"])
    else:
        df['CreatedOn'] = df['CreatedOn'].map(lambda x: pd.to_datetime(x[:19]))
        return df[["BfpProvidedBankAccountId",
                   "CreatedOn", "Balance"]]


def computeAvgCycle(time_series):
    if time_series.empty:
        return 365
    elif time_series.shape[0] == 1:
        return 90
    else:
        return time_series.diff().mean().days


def computeCycleStd(time_series):
    if time_series.empty:
        return 365
    elif time_series.shape[0] <= 2:
        return -1
    else:
        return time_series.diff().std().days


def extractAHTransactions(df_in, col='Description'):
    df_out = df_in[(isAHTransaction(df_in[col]))]
    return df_out


def dropAHTransactions(df_in, col='Description'):
    df_out = df_in[(isNotAHTransaction(df_in[col]))]
    return df_out


def isAHTransaction(series):
    return series.map(lambda x: 'activehour' in x.lower())


def isNotAHTransaction(series):
    return series.map(lambda x: 'activehour' not in x.lower())


def detectAccountKeyWord(string):
    string = string.lower()
    if 'checking' in string:
        return 'checking'
    elif 'saving' in string:
        return 'saving'
    elif 'credit' in string:
        return 'credit'
    elif 'loan' in string:
        return 'loan'
    else:
        return 'unknown'

sqlalchemy_metadata_map = {}


def _get_metadata(db_name):
    if (db_name not in sqlalchemy_metadata_map):
        sqlalchemy_metadata_map[db_name] = MetaData()
    return sqlalchemy_metadata_map[db_name]


def _get_table(db_name, table_name):
    metadata = _get_metadata(db_name)
    schema = ah_config.get('database.' + db_name + '.schema', '')
    full_table_name = (schema + '.' if schema else '') + table_name
    if (full_table_name not in metadata.tables):
        with ah_db.open_db_engine(db_name) as engine:
            logging.getLogger('ah_db.misc').info("Creating table %s from %s", full_table_name, db_name)
            table = Table(table_name, metadata, autoload=True, autoload_with=engine, schema=schema)

            #   See if this is a table with a sequence primary key
            sequence_tables = ah_config.get('database.sequenceTables', [])
            table.is_sequence = table_name.lower() in sequence_tables


    return metadata.tables[full_table_name]


def _lower_data(data):
    result = {}
    for key, value in data.items():
        result[key.lower()] = value
    return result


def _match_data_to_column_names(table, lower_data):
    #   Match data to column names
    column_data = {}
    for column in table.c:
        lower_name = column.name.lower()
        if (lower_name in lower_data):
            column_data[column.name] = lower_data[lower_name]
    return column_data


last_secondary_write_check = 0
do_secondary_write = True
def _do_secondary_write():
    global last_secondary_write_check, do_secondary_write

    now = int(time.time())
    if now < last_secondary_write_check:
        return do_secondary_write

    last_secondary_write_check = now + 30

    with ah_db.open_db_connection('sqlserver') as connection:
        sql = "select ParallelWritingOn from ops.AuroraMigrationConfig where DomainName = 'DataScience'"
        row = connection.execute(sql).fetchone()
        if row:
            do_secondary_write = row[0]
        else:
            do_secondary_write = False

    logging.getLogger('ah_db.misc').info("Secondary write check %s", do_secondary_write)
    return do_secondary_write


def migration_insert(sqlserver_db, aurora_db, table_name, data):

    #   Normalize the column names
    lower_data = _lower_data(data)

    #   See if there is a primary writer specific to this table
    primary = ah_config.get('database.primaryWriter.%s' % table_name, ah_config.get('database.primaryWriter', 'sqlserver'))
    if (primary == 'sqlserver'):
        primary_db_name = sqlserver_db
        secondary_db_name = aurora_db
    else:
        primary_db_name = aurora_db
        secondary_db_name = sqlserver_db

    primary_table = _get_table(primary_db_name, table_name)
    secondary_table = _get_table(secondary_db_name, table_name)

    with ah_db.open_db_connection(primary_db_name) as primary_connection:
        #   Insert into the primary
        column_data = _match_data_to_column_names(primary_table, lower_data)
        ins = primary_table.insert().values(column_data)
        result = primary_connection.execute(ins)
        primary_keys = result.inserted_primary_key
        new_pk_value = primary_keys[0] if len(primary_keys) > 0 else 'n/a'
        logging.getLogger('ah_db.misc').info("Inserted into primary %s.%s: %s", primary_db_name, table_name, new_pk_value)

        if not _do_secondary_write():
            return new_pk_value

        with ah_db.open_db_connection(secondary_db_name) as secondary_connection:
            #   Now insert into secondary. See if this is a 'sequence table'
            if (secondary_table.is_sequence):

                #   Since this is a "sequence" table, first get the Primary Key (we assume it is a single column)
                pk_col = list(secondary_table.primary_key.columns)[0]
                lower_data[pk_col.name.lower()] = new_pk_value

                #   Try to do the insert. We may get an exception
                try:
                    column_data = _match_data_to_column_names(secondary_table, lower_data)
                    ins = secondary_table.insert().values(column_data)
                    result = secondary_connection.execute(ins)
                    logging.getLogger('ah_db.misc').info("Inserted into secondary %s.%s: %s",
                                                         secondary_db_name, table_name, result.inserted_primary_key[0])

                except:
                    #   Let's do an update
                    update = secondary_table.update().where(pk_col == new_pk_value).values(column_data)
                    secondary_connection.execute(update)
                    logging.getLogger('ah_db.misc').info("Updated into secondary %s.%s: %s",
                                                         secondary_db_name, table_name, new_pk_value)
            else:

                #   This is not a "sequence" table. We just need to do an insert
                column_data = _match_data_to_column_names(secondary_table, lower_data)
                ins = secondary_table.insert().values(column_data)
                secondary_connection.execute(ins)
                logging.getLogger('ah_db.misc').info("Inserted into secondary %s.%s", secondary_db_name, table_name)

        return new_pk_value


def migration_update(sqlserver_db, aurora_db, table_name, data, where_clause):

    #   Normalize the column names
    lower_data = _lower_data(data)

    #   See if there is a primary writer specific to this table
    primary = ah_config.get('database.primaryWriter.%s' % table_name, ah_config.get('database.primaryWriter', 'sqlserver'))
    if (primary == 'sqlserver'):
        primary_db_name = sqlserver_db
        secondary_db_name = aurora_db
    else:
        primary_db_name = aurora_db
        secondary_db_name = sqlserver_db

    primary_table = _get_table(primary_db_name, table_name)
    secondary_table = _get_table(secondary_db_name, table_name)

    with ah_db.open_db_connection(primary_db_name) as primary_connection:
        #   First, the primary
        column_data = _match_data_to_column_names(primary_table, lower_data)
        update = primary_table.update().where(text(where_clause)).values(column_data)
        primary_connection.execute(update)

        if _do_secondary_write():
            #   Now, the secondary
            with ah_db.open_db_connection(secondary_db_name) as secondary_connection:
                column_data = _match_data_to_column_names(secondary_table, lower_data)
                update = secondary_table.update().where(text(where_clause)).values(column_data)
                secondary_connection.execute(update)


def migration_select(sqlserver_db, aurora_db, table_name, sqlserver_query, aurora_query=None):

    log = ah_config.getLogger('ah.dbmigration')

    #   See if there is a primary reader specific to this table
    primary = ah_config.get('database.primaryReader.%s' % table_name, ah_config.get('database.primaryReader', 'sqlserver'))

    if primary == 'sqlserver':
        log.info('Migration read table {0} from sqlserver. Primary is {1}'.format(table_name, primary))
        with ah_db.open_db_connection(sqlserver_db) as connection:
            result = connection.execute(sqlserver_query)
            return result.fetchall()

    elif primary == 'aurora':
        log.info('Migration read table {0} from aurora. Primary is {1}'.format(table_name, primary))
        if (aurora_query == None):
            aurora_query = sqlserver_query
        with ah_db.open_db_connection(aurora_db) as connection:
            result = connection.execute(aurora_query)
            return result.fetchall()

    else:
        log.exception('Invalid primary DB for table {0}. Primary is {1}'.format(table_name, primary))


def migration_select_df(sqlserver_db, aurora_db, table_name, sqlserver_query, aurora_query=None):

    log = ah_config.getLogger('ah.dbmigration')

    #   See if there is a primary reader specific to this table
    primary = ah_config.get('database.primaryReader.%s' % table_name, ah_config.get('database.primaryReader', 'sqlserver'))

    if primary == 'sqlserver':
        log.info('Migration read table {0} from sqlserver. Primary is {1}'.format(table_name, primary))
        with ah_db.open_db_connection(sqlserver_db) as connection:
            return pd.read_sql(sqlserver_query, con=connection)

    elif primary == 'aurora':
        log.info('Migration read table {0} from aurora. Primary is {1}'.format(table_name, primary))
        if (aurora_query == None):
            aurora_query = sqlserver_query
        with ah_db.open_db_connection(aurora_db) as connection:
            return pd.read_sql(aurora_query, con=connection)

    else:
        log.exception('Invalid primary DB for table {0}. Primary is {1}'.format(table_name, primary))


def migration_delete(sqlserver_db, aurora_db, table_name, where_clause):

    #   See if there is a primary writer specific to this table
    primary = ah_config.get('database.primaryWriter.%s' % table_name, ah_config.get('database.primaryWriter', 'sqlserver'))
    if (primary == 'sqlserver'):
        primary_db_name = sqlserver_db
        secondary_db_name = aurora_db
    else:
        primary_db_name = aurora_db
        secondary_db_name = sqlserver_db

    primary_table = _get_table(primary_db_name, table_name)
    secondary_table = _get_table(secondary_db_name, table_name)

    with ah_db.open_db_connection(primary_db_name) as primary_connection:
        with ah_db.open_db_connection(secondary_db_name) as secondary_connection:
            #   First, the primary
            delete = primary_table.delete().where(text(where_clause))
            primary_connection.execute(delete)

            #   Now, the secondary
            delete = secondary_table.delete().where(text(where_clause))
            secondary_connection.execute(delete)


def sql_insert(db_name, table_name, data):
    lower_data = _lower_data(data)
    table = _get_table(db_name, table_name)
    column_data = _match_data_to_column_names(table, lower_data)
    
    with ah_db.open_db_connection(db_name) as conn:
        ins = table.insert().values(column_data)
        conn.execute(ins)


def sql_update(db_name, table_name, data, where_clause):
    lower_data = _lower_data(data)
    table = _get_table(db_name, table_name)
    column_data = _match_data_to_column_names(table, lower_data)

    with ah_db.open_db_connection(db_name) as conn:
        update = table.update().where(text(where_clause)).values(column_data)
        conn.execute(update)


def sql_select(db_name, table_name, query):
    with ah_db.open_db_connection(db_name) as conn:
        result = conn.execute(query)
        return result.fetchall()


def sql_select_df(db_name, table_name, query):
    with ah_db.open_db_connection(db_name) as conn:
        return pd.read_sql(query, con=conn)


def sql_delete(db_name, table_name, where_clause):
    table = _get_table(db_name, table_name)
    with ah_db.open_db_connection(db_name) as conn:
            delete = table.delete().where(text(where_clause))
            conn.execute(delete)


