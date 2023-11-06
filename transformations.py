from sqlalchemy import create_engine
from queries import Query
import pandas as pd
import argparse

engine = create_engine('sqlite:///gig_spotify.db',echo=True)

# Class for performing data transformations
class Transformations:
    # Constructor
    def __init__(self, transformation):
        self.transformation = transformation

    # Function to get country values
    def get_country_val(self):
        with engine.connect() as connection:
            res1 = connection.execute(Query.query_drop_table.format(table_name="playlist_country"))
            result = connection.execute(Query.query_create_country_col)
            print(result)

    # Function to extract audio features by country
    def audio_features_by_country(self):
        df_top = pd.read_sql_query(Query.query_af_top, engine)
        df_viral = pd.read_sql_query(Query.query_af_viral, engine)
        df_viral.to_sql(name="country_ft_viral",con=engine,if_exists="replace",index=False)
        df_top.to_sql(name="country_ft_top",con=engine,if_exists="replace",index=False)

    # Function to find most popular daily tracks
    def most_popular_daily(self):
        df = pd.read_sql_query(Query.query_most_popular_daily, engine)
        df.to_sql(name="daily_most_popular",con=engine,if_exists="replace",index=False)

    # Function to calculate country statistics
    def country_statistics(self):
        df = pd.read_sql_query(Query.query_avg_statistics_by_country, engine)
        df.to_sql(name="country_statistics",con=engine,if_exists="replace",index=False)

    # Function to find distinct songs
    def distinct_songs(self):
        df = pd.read_sql_query(Query.query_distinct_songs, engine)
        df.to_sql(name="distinct_tracks",con=engine,if_exists="replace",index=False)

    # Function to identify artists with many songs
    def artists_many_songs(self):
        df = pd.read_sql_query(Query.query_poplar_artist_songs_ft, engine)
        df.to_sql(name="certain_tracks_ft",con=engine,if_exists="replace",index=False)        

    # Function to run the specified transformation
    def run(self):
        if self.transformation == "val":
            self.get_country_val()
        elif self.transformation == "countryft":
            self.audio_features_by_country()
        elif self.transformation == "popular":
            self.most_popular_daily()
        elif self.transformation == "statistics":
            self.country_statistics()
        elif self.transformation == "distinct":
            self.distinct_songs()
        elif self.transformation == "certain":
            self.artists_many_songs()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script which gets data from the Spotify API.')
    parser.add_argument('arg', type=str, help='Can be val, countryft, popular, statistics, distinct, certain')
    args = parser.parse_args()
    tr = Transformations(transformation=args.arg)
    tr.run()
