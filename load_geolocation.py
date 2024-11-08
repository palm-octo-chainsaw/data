from notebooks.config import (
    pd,
    create_engine,
    DB_CONNECTION_STRING
)


TABLE_NAME = 'geolocations'


def load_geolocation_data(json: str) -> pd.DataFrame:
    data = pd.read_json(json)
    cleaned_data = pd.DataFrame()

    cleaned_data['id'] = data['id']
    cleaned_data['country'] = data['country']
    cleaned_data['region'] = data.apply(lambda row: row['country'] if pd.isnull(row['region']) else row['region'], axis=1)
    cleaned_data['location'] = data['location']
    cleaned_data['timezone'] = data['timezone']

    normalize_loc = pd.json_normalize(cleaned_data['location'])

    cleaned_data[[col for col in normalize_loc.columns]] = normalize_loc
    cleaned_data.drop(columns=['location'], inplace=True, axis=1)

    return cleaned_data


data = load_geolocation_data('notebooks/full.json')
engine = create_engine(DB_CONNECTION_STRING)
data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
engine.dispose()
