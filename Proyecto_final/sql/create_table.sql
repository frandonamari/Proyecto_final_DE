DROP TABLE IF EXISTS fran_d_donamari_coderhouse.top_50_global_stg;
CREATE TABLE fran_d_donamari_coderhouse.top_50_global_stg (
            track_id	VARCHAR(50) primary key DISTKEY,
            track_name	VARCHAR(100),
            artist_name	VARCHAR(50),
            album_name	VARCHAR(100),
            album_release_date	VARCHAR(10),
            album_popularity VARCHAR(100)
        )diststyle KEY
        sortkey (track_id)
       	; 

DROP TABLE IF EXISTS fran_d_donamari_coderhouse.top_50_global_dim;
CREATE TABLE fran_d_donamari_coderhouse.top_50_global_dim (
            track_id	VARCHAR(50) DISTKEY,
            track_name	VARCHAR(100),
            artist_name	VARCHAR(50),
            album_name	VARCHAR(100),
            album_release_date VARCHAR(10),
            album_popularity VARCHAR(100),
            updated_at	TIMESTAMP,
            created_at	TIMESTAMP
        )diststyle KEY
        sortkey (track_id)
        ;