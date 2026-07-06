import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node artist
artist_node1768654017650 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhinab/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1768654017650")

# Script generated for node album
album_node1768654038945 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhinab/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1768654038945")

# Script generated for node track
track_node1768654040614 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-abhinab/staging/track.csv"], "recurse": True}, transformation_ctx="track_node1768654040614")

# Script generated for node Join album and artist
Joinalbumandartist_node1768654420537 = Join.apply(frame1=album_node1768654038945, frame2=artist_node1768654017650, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumandartist_node1768654420537")

# Script generated for node Join with track
Joinwithtrack_node1768654937525 = Join.apply(frame1=Joinalbumandartist_node1768654420537, frame2=track_node1768654040614, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtrack_node1768654937525")

# Script generated for node Drop Fields
DropFields_node1768655159317 = DropFields.apply(frame=Joinwithtrack_node1768654937525, paths=["id", "`.track_id`"], transformation_ctx="DropFields_node1768655159317")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1768655159317, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768653610113", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1768655285040 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1768655159317, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-project-abhinab/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1768655285040")

job.commit()