from datetime import timedelta
from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
)
from feast.types import Float32, Int64, String
from feast.value_type import ValueType

# Define entity
packet = Entity(
    name="packet", 
    join_keys=["packet_id"],
    value_type=ValueType.STRING
)

# Define data source
packet_source = FileSource(
    name="packet_source",
    path="../data/kdd_train.parquet",
    timestamp_field="event_timestamp",
)

# Define entity feature view
packet_features = FeatureView(
    name="packet_stats",
    entities=[packet],
    ttl=timedelta(days=1),
    schema=[
        Field(name="src_bytes", dtype=Float32),
        Field(name="dst_bytes", dtype=Float32),
        Field(name="duration", dtype=Float32),
        Field(name="count", dtype=Float32),
        Field(name="srv_count", dtype=Float32),
        Field(name="protocol_type", dtype=String),
        Field(name="service", dtype=String),
        Field(name="flag", dtype=String),
    ],
    online=True,  #? Enable syncing to Redis
    source=packet_source,
    tags={"team": "security"},
)