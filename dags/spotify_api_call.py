from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
}

with DAG('spotify_api_call', default_args=default_args, schedule_interval='@daily') as dag:
    task_get_top_playlists = BashOperator(
        task_id='get_daily_top_playlists',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python main.py p
        """,
    )
    task_get_audio_features = BashOperator(
        task_id='get_features',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python main.py f
        """,
    )
    task_get_country_val = BashOperator(
        task_id='get_country_val',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py val
        """,
    )
    task_get_country_features = BashOperator(
        task_id='get_country_features',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py countryft
        """,
    )
    task_get_popular = BashOperator(
        task_id='get_popular',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py popular
        """,
    )
    task_get_country_stats = BashOperator(
        task_id='get_country_sats',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py statistics
        """,
    )
    task_get_artists_distinct_songs = BashOperator(
        task_id='get_distinct',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py distinct
        """,
    )
    task_get_artists_many_songs = BashOperator(
        task_id='get_many',
        bash_command="""
            cd /home/thorsteinnj/Documents/Git/gig_project && \
            source /home/thorsteinnj/anaconda3/bin/activate gig && \
            python transformations.py certain
        """,
    )

    task_get_top_playlists >> task_get_audio_features
    task_get_top_playlists >> task_get_country_val
    task_get_top_playlists >> task_get_popular
    task_get_top_playlists >> task_get_country_stats
    task_get_top_playlists >> task_get_artists_distinct_songs
    task_get_audio_features >> task_get_country_features
    task_get_audio_features >> task_get_artists_many_songs

    