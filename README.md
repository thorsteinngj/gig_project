# Project done in Spotify API

## Description
This project gets data about the Top 50 and Viral 50 playlists on Spotify, on a daily basis, and gathers insight about the tracks, features of the tracks and publishes those in an SQLite database.

## Installation
For setup of this project, the dags/ folder should be placed in the correct Airflow folder. It is included here just for the purpose of including it. The main file takes care of gathering data about tracks in the country Top playlists on Spotify as well as features of the tracks. An attempt was made on gathering data about the artists, but the API endpoint for that did not work well.

To setup, please create a venv or conda environment and install the requirements.