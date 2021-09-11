# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
summoner_stats = FileSource(
    path="/Users/yeungadrian/Documents/repo/ML-Ops/features/data/summoners.parquet",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(
    name="summonerId",
    value_type=ValueType.STRING,
    description="summoner id",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
summoner_stats_view = FeatureView(
    name="summoner_stats_view",
    entities=["summonerId"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="tier", dtype=ValueType.STRING),
        Feature(name="wins", dtype=ValueType.INT64),
        Feature(name="leaguePoints", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=summoner_stats,
    tags={},
)
