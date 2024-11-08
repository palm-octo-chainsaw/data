from notebooks.config import (
    pd,
    create_engine,
    DB_CONNECTION_STRING
)


COLUMNS_TO_DROP = ['identifiers', 'location', 'timezone', 'inventory', 'name', 'country', 'region']
TABLE_NAME = 'station_names'
data = pd.read_json('notebooks/full.json')


def normalize_name(data: pd.DataFrame) -> pd.DataFrame:
    normalize_name = pd.json_normalize(data['name'])
    normalize_name = normalize_name.where(pd.notna(normalize_name), None)
    data[[col for col in normalize_name]] = normalize_name
    return data


def normalize_identifiers(data: pd.DataFrame) -> pd.DataFrame:
    normalize = pd.json_normalize(data['identifiers'])
    data[[col for col in normalize]] = normalize
    return data


def clean_data(data: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
    return data.drop(columns=columns_to_drop, inplace=True, axis=1)


normalize_name(data)
clean_data(data, COLUMNS_TO_DROP)
engine = create_engine(DB_CONNECTION_STRING)
data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
engine.dispose()
