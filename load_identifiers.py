import logging

from notebooks.config import (
    pd,
    create_engine,
    DB_CONNECTION_STRING
)


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("identifiers.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


TABLE_NAME = 'identifiers'
COLUMN_TO_NORMALIZE = 'identifiers'

COLUMNS_TO_INTEGER = ['monthly.start', 'monthly.end',
                      'normals.start', 'normals.end']

COLUMNS_TO_DATETIME = ['model.start', 'model.end',
                       'hourly.start', 'hourly.end',
                       'daily.start', 'daily.end']

COLUMNS_TO_DROP = ['name', 'inventory',
                   'country', 'location',
                   'region', 'timezone']


def main():
    logging.info('Starting ETL...')

    data = pd.read_json('full.json')
    data.drop(columns=COLUMNS_TO_DROP, inplace=True)
    logging.info('Columns droped...')

    normal = pd.json_normalize(data['identifiers'])
    data.drop(columns=COLUMN_TO_NORMALIZE, inplace=True)
    data[[column for column in normal]] = normal
    logging.info(f'Normalized [{COLUMN_TO_NORMALIZE}] column...')

    logging.info('Loading transformed data...')
    engine = create_engine(DB_CONNECTION_STRING)
    data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    engine.dispose()
    logging.info('Successfuly ran ETL...')


if __name__ == '__main__':
    main()
