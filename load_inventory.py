from config import (
    pd,
    create_engine,
    DB_CONNECTION_STRING
)


TABLE_NAME = 'inventory'
COLUMN_TO_NORMALIZE = 'inventory'
COLUMNS_TO_INTEGER = ['monthly.start', 'monthly.end',
                      'normals.start', 'normals.end',]

COLUMNS_TO_DATETIME = ['model.start', 'model.end',
                       'hourly.start', 'hourly.end',
                       'daily.start', 'daily.end',]

COLUMNS_TO_DROP = ['name', 'identifiers',
                   'country', 'location',
                   'region', 'timezone',]


class Modify:
    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.dataframe = dataframe

    def __call__(self) -> pd.DataFrame:
        return self.dataframe

    def __str__(self) -> str:
        return f'[Modify: {type(self.dataframe)}]'

    def drop_columns(self, columns: list) -> pd.DataFrame:
        """Drops tables from dataframe"""
        self.dataframe.drop(columns, inplace=True, axis=1)
        return self.dataframe

    def normalize(self, column: str) -> pd.DataFrame:
        """Normalizes a nested column"""
        normal = pd.json_normalize(self.dataframe[column])
        self.dataframe.drop(column, axis=1, inplace=True)
        self.dataframe[[col for col in normal.columns]] = normal
        return self.dataframe

    def convert_to_integer(self, columns_to_convert: list) -> pd.DataFrame:
        """Converts columns to integer data type"""
        self.dataframe[columns_to_convert] = self.dataframe[columns_to_convert].apply(lambda col: col.astype('Int64'))
        return self.dataframe

    def convert_to_datetime(self, columns_to_convert: list) -> pd.DataFrame:
        """Converts columns to datetime data type"""
        self.dataframe[columns_to_convert] = self.dataframe[columns_to_convert].apply(pd.to_datetime, errors='coerce')
        return self.dataframe

    def load_to_sql(self, connection: str, db_table: str) -> str:
        """Loads dataframe to SQL database"""
        engine = create_engine(connection)
        self.dataframe.to_sql(db_table, engine, if_exists='replace', index=False)
        engine.dispose()
        return f"Successfuly loaded {len(self.dataframe)} row to [{db_table}] table."


data = Modify(
    dataframe=pd.read_json('full.json'),
)

data.drop_columns(COLUMNS_TO_DROP)
data.normalize(COLUMN_TO_NORMALIZE)
data.convert_to_integer(COLUMNS_TO_INTEGER)
data.convert_to_datetime(COLUMNS_TO_DATETIME)
data.load_to_sql(DB_CONNECTION_STRING, TABLE_NAME)
