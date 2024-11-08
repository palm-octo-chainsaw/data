from config import *


table_name = 'station_names'
columns_to_integer = ['monthly.start', 'monthly.end', 'normals.start', 'normals.end']
columns_to_date = ['model.start', 'model.end', 'hourly.start', 'hourly.end', 'daily.start', 'daily.end']
columns_to_drop = ['name', 'identifiers', 'country', 'location', 'region', 'timezone']

class Modify:
    def __init__(
            self,
            dataframe: pd.DataFrame,
        ) -> None:
        self.dataframe = dataframe

    def __call__(self) -> pd.DataFrame:
        return self.dataframe
    
    def __str__(self) -> str:
        return f'[Modify: {type(self.dataframe)}]'

    def drop_columns(self, columns: list) -> pd.DataFrame:
        self.dataframe.drop(columns, inplace=True, axis=1)
        return self.dataframe

    def normalize(self, column: str) -> pd.DataFrame:
        normal = pd.json_normalize(self.dataframe[column])
        self.dataframe.drop(column, axis=1, inplace=True)
        self.dataframe[[col for col in normal.columns]] = normal
        return self.dataframe

    def convert_to_integer(self, columns_to_convert: list) -> pd.DataFrame:
        self.dataframe[columns_to_convert] = self.dataframe[columns_to_convert].apply(lambda col: col.astype('Int64'))
        return self.dataframe

    def convert_to_datetime(self, columns_to_convert: list) -> pd.DataFrame:
        self.dataframe[columns_to_convert] = self.dataframe[columns_to_convert].apply(pd.to_datetime, errors='coerce')
        return self.dataframe
    
    def load_to_sql(self, connection_string: str, db_table: str) -> str:
        engine = create_engine(connection_string)
        self.dataframe.to_sql(db_table, engine, if_exists='replace', index=False)
        engine.dispose()
        return f"Successfuly loaded {len(self.dataframe)} row to [{db_table}] table."


data = Modify(
    dataframe=pd.read_json('full.json'),
)

print(data)

# data.drop_columns(columns_to_drop)
# data.normalize('inventory')
# data.convert_to_integer(columns_to_integer)
# data.convert_to_datetime(columns_to_date)
# data.load_to_sql(connection_string, 'inventory')

