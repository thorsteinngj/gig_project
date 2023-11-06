class Query:
    query_playlists = """SELECT 
                            name, 
                            playlist_id
                        FROM playlists 
                        WHERE 1=1 
                            AND owner="Spotify"
                        """
    
    query_artists = """WITH new_artists AS 
                        (
                            SELECT 
                                artist, 
                                artist_id 
                            FROM playlist_tracks 
                            WHERE 1=1 
                                AND upload_time = (SELECT MAX(upload_time) FROM playlist_tracks)
                        ),
                        old_artists AS 
                        (
                            SELECT
                                artist_id 
                            FROM playlist_tracks 
                            WHERE 1=1 
                                AND upload_time < (SELECT MAX(upload_time) FROM playlist_tracks)
                        )
                        SELECT 
                            artist,
                            artist_id
                        FROM new_artists
                        WHERE 1=1
                            AND artist_id NOT IN (SELECT artist_id FROM old_artists);"""
    
    query_drop_table = """DROP TABLE IF EXISTS {table_name};"""
                            
    query_create_country_col = """CREATE TABLE playlist_country AS 
                                SELECT 
                                    artist,
                                    track_name,
                                    track_id,
                                    popularity,
                                    release_date,
                                    duration,
                                    upload_time,
                                    SUBSTR(playlist, INSTR(playlist, ' - ') + 2)  AS country_name,
                                    SUBSTR(playlist, 1, INSTR(playlist, ' ') -1)  AS playlist_type
                                FROM playlist_tracks;"""
    
    query_tracks = """WITH new_artists AS 
                        (
                            SELECT 
                                track_name, 
                                track_id 
                            FROM playlist_tracks 
                            WHERE 1=1 
                                AND upload_time = (SELECT MAX(upload_time) FROM playlist_tracks)
                        ),
                        old_artists AS 
                        (
                            SELECT
                                track_id 
                            FROM playlist_tracks 
                            WHERE 1=1 
                                AND upload_time < (SELECT MAX(upload_time) FROM playlist_tracks)
                        )
                        SELECT 
                            track_name,
                            track_id
                        FROM new_artists
                        WHERE 1=1
                            AND track_id NOT IN (SELECT track_id FROM old_artists);"""
    
    query_tracks_init = """
                            SELECT DISTINCT
                                track_id 
                            FROM playlist_tracks;
                            """
    
    query_af_top = """SELECT 
            pc.country_name,
            af.danceability,
            af.energy,
            af.tempo,
            af.speechiness,
            af.acousticness,
            af.instrumentalness,
            af.liveness,
            af.valence
        FROM playlist_country pc
        JOIN audio_features af
        ON pc.track_id = af.track_id
        WHERE 1=1
            AND pc.playlist_type = "Top"
        GROUP BY pc.country_name;"""
    
    query_af_viral = """SELECT 
            pc.country_name,
            af.danceability,
            af.energy,
            af.tempo,
            af.speechiness,
            af.acousticness,
            af.instrumentalness,
            af.liveness,
            af.valence
        FROM playlist_country pc
        JOIN audio_features af
        ON pc.track_id = af.track_id
        WHERE 1=1
            AND pc.playlist_type = "Viral"
        GROUP BY pc.country_name;"""
    
    query_most_popular_daily = """SELECT 
                        artist, 
                        upload_time,
                        COUNT(DISTINCT country_name) AS num_countries
                    FROM playlist_country
                    GROUP BY artist, upload_time
                    ORDER BY num_countries DESC;"""
    
    query_avg_statistics_by_country = """
                    SELECT 
                        country_name,
                        playlist_type,
                        AVG(min_date) AS oldest_song,
                        avg_popularity,
                        avg_release_date
                    FROM (
                        SELECT 
                            country_name,
                            playlist_type,
                            AVG(popularity) AS avg_popularity,
                            AVG(release_date) AS avg_release_date,
                            MIN(release_date) AS min_date
                        FROM playlist_country
                        GROUP BY country_name
                    ) t
                    GROUP BY country_name, playlist_type
                    ORDER BY avg_release_date ASC
                """
    
    query_distinct_songs = """
                SELECT 
                    DATE(upload_time) AS date,
                    playlist_type,
                    AVG(popularity) AS avg_popularity,
                    artist,
                    COUNT(DISTINCT track_name) AS num_distinct_songs
                FROM playlist_country
                WHERE 1=1
                    AND artist!="Various Artists"
                GROUP BY date, artist, playlist_type
                ORDER BY num_distinct_songs DESC;
            """
    
    query_poplar_artist_songs_ft = """WITH popular_artist_id AS (
    SELECT DISTINCT
        distinct_songs.artist,
        playlist_tracks.artist_id
    FROM distinct_songs
    LEFT JOIN playlist_tracks ON playlist_tracks.artist = distinct_songs.artist
    WHERE 1=1
        AND num_distinct_songs >= 10),
    popular_artist_songs AS (
        SELECT DISTINCT
            track_name,
            artist_id,
            track_id
        FROM playlist_tracks),
    popular_songs_features AS (
        SELECT DISTINCT
            track_id,
            danceability,
            energy,
            tempo,
            speechiness,
            acousticness,
            instrumentalness,
            liveness,
            valence
        FROM audio_features
    )

    SELECT *
    FROM popular_artist_id
    JOIN popular_artist_songs 
    ON popular_artist_id.artist_id = popular_artist_songs.artist_id
    JOIN popular_songs_features
    ON popular_songs_features.track_id = popular_artist_songs.track_id;"""

    query_select_all = """
                        SELECT * FROM {table_name}"""