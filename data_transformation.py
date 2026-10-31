# Import Python and AWS Glue / PySpark libraries
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame


# Initialize Spark and AWS Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Define the S3 path containing raw Spotify JSON data
s3_path = "s3://spotify-etl-project-gaurav-ambasta/raw_data/to_processed/"


# Read raw Spotify JSON data from S3 into a Glue DynamicFrame
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
)


# Convert the DynamicFrame into a Spark DataFrame
spotify_df = source_dyf.toDF()
df = spotify_df


# Extract album details and remove duplicate albums using album_id
def process_albums(df):
    df = df.withColumn("items", explode("items")).select(
        col("items.item.album.id").alias("album_id"),
        col("items.item.album.name").alias("album_name"),
        col("items.item.album.release_date").alias("release_date"),
        col("items.item.album.total_tracks").alias("total_tracks"),
        col("items.item.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])

    return df


# Extract artist details by expanding the artists array
# Remove duplicate artists using artist_id
def process_artist(df):
    df_items_exploded = df.select(
        explode(col("items")).alias("item")
    )

    df_artist_exploded = df_items_exploded.select(
        explode(col("item.item.artists")).alias("artist")
    )

    df_artists = df_artist_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("artist_url")
    ).drop_duplicates(["artist_id"])

    return df_artists


# Extract song details, including album_id and the first artist's ID
# Remove duplicate songs using song_id and convert added_at to a date
def process_songs(df):
    df_explode = df.select(
        explode(col("items")).alias("item")
    )

    df_songs = df_explode.select(
        col("item.item.id").alias("song_id"),
        col("item.item.name").alias("song_name"),
        col("item.item.duration_ms").alias("duration_ms"),
        col("item.item.external_urls.spotify").alias("url"),
        col("item.added_at").alias("added_at"),
        col("item.item.album.id").alias("album_id"),
        col("item.item.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(["song_id"])

    df_songs = df_songs.withColumn(
        "added_at",
        col("added_at").cast("date")
    )

    return df_songs


# Run the three transformation functions to create separate DataFrames
album_df = process_albums(spotify_df)
artist_df = process_artist(spotify_df)
song_df = process_songs(spotify_df)


# Convert a Spark DataFrame to a DynamicFrame and write it to S3
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(
        df, glueContext, "dynamic_frame"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": (
                f"s3://spotify-etl-project-gaurav-ambasta/"
                f"transformed_data/{path_suffix}/"
            )
        },
        format=format_type
    )


# Write transformed album data to a date-stamped S3 location
write_to_s3(
    album_df,
    "album_data/album_transformed_{}".format(
        datetime.now().strftime("%Y-%m-%d")
    ),
    "csv"
)


# Write transformed artist data to a date-stamped S3 location
write_to_s3(
    artist_df,
    "artist_data/artist_transformed_{}".format(
        datetime.now().strftime("%Y-%m-%d")
    ),
    "csv"
)


# Write transformed song data to a date-stamped S3 location
write_to_s3(
    song_df,
    "song_data/song_transformed_{}".format(
        datetime.now().strftime("%Y-%m-%d")
    ),
    "csv"
)


def list_s3_objects(bucket_name, prefix):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')

    keys = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        keys.extend(
            content['Key']
            for content in page.get('Contents', [])
            if content['Key'].endswith('.json')
        )

    return keys


bucket_name = 'spotify-etl-project-gaurav-ambasta'
prefix = 'raw_data/to_processed/'

json_files = list_s3_objects(bucket_name, prefix)


def move_and_delete_files(json_files, bucket_name):
    s3_resource = boto3.resource('s3')

    for file_key in json_files:
        copy_source = {
            'Bucket': bucket_name,
            'Key': file_key
        }

        destination_key = (
            'raw_data/processed/' + file_key.split('/')[-1]
        )

        # Copy the raw JSON file to processed/
        s3_resource.meta.client.copy(
            copy_source, bucket_name, destination_key
        )

        # Delete the original only after successful copy
        s3_resource.Object(bucket_name, file_key).delete()


# Execute the archival operation
move_and_delete_files(json_files, bucket_name)


# Commit the Glue job
job.commit()



