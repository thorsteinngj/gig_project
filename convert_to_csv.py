import pandas as pd
from sqlalchemy import create_engine
from queries import Query
import os
cwd = os.getcwd()

engine = create_engine('sqlite:///gig_spotify.db',echo=True)

tables = ["playlist_tracks","audio_features","distinct_songs","playlist_country","country_ft_viral","country_ft_top","daily_most_popular","country_statistics","distinct_tracks","certain_tracks_ft"]

for i in range(len(tables)):
    query = Query.query_select_all.format(table_name=tables[i])
    path = cwd+"/data/"+tables[i]+".csv"

    df = pd.read_sql(query,con=engine)
    df.to_csv(path)

    print(df.head())