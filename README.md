from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Assuming 'df' is your DataFrame
df = df.select(
    *[col(c).cast(StringType()).alias(c) if df.schema[c].dataType == 'void' else col(c) for c in df.columns]
)


