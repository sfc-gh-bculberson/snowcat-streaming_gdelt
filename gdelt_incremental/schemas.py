"""GDELT 2.0 table schemas, aligned with the Google BigQuery gdelt-bq.gdeltv2
column layout (same layout used by ../streaming_gdelt for the bulk loader, so
EVENTS/EVENTMENTIONS/GKG stay compatible across both loaders)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Tuple

ColumnKind = Literal["INTEGER", "FLOAT", "STRING"]
TableKind = Literal["events", "mentions", "gkg"]

TABLE_ORDER: Tuple[TableKind, ...] = ("events", "mentions", "gkg")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: ColumnKind


EVENTS_COLUMNS: Tuple[ColumnSpec, ...] = (
    ColumnSpec("GLOBALEVENTID", "INTEGER"),
    ColumnSpec("SQLDATE", "INTEGER"),
    ColumnSpec("MonthYear", "INTEGER"),
    ColumnSpec("Year", "INTEGER"),
    ColumnSpec("FractionDate", "FLOAT"),
    ColumnSpec("Actor1Code", "STRING"),
    ColumnSpec("Actor1Name", "STRING"),
    ColumnSpec("Actor1CountryCode", "STRING"),
    ColumnSpec("Actor1KnownGroupCode", "STRING"),
    ColumnSpec("Actor1EthnicCode", "STRING"),
    ColumnSpec("Actor1Religion1Code", "STRING"),
    ColumnSpec("Actor1Religion2Code", "STRING"),
    ColumnSpec("Actor1Type1Code", "STRING"),
    ColumnSpec("Actor1Type2Code", "STRING"),
    ColumnSpec("Actor1Type3Code", "STRING"),
    ColumnSpec("Actor2Code", "STRING"),
    ColumnSpec("Actor2Name", "STRING"),
    ColumnSpec("Actor2CountryCode", "STRING"),
    ColumnSpec("Actor2KnownGroupCode", "STRING"),
    ColumnSpec("Actor2EthnicCode", "STRING"),
    ColumnSpec("Actor2Religion1Code", "STRING"),
    ColumnSpec("Actor2Religion2Code", "STRING"),
    ColumnSpec("Actor2Type1Code", "STRING"),
    ColumnSpec("Actor2Type2Code", "STRING"),
    ColumnSpec("Actor2Type3Code", "STRING"),
    ColumnSpec("IsRootEvent", "INTEGER"),
    ColumnSpec("EventCode", "STRING"),
    ColumnSpec("EventBaseCode", "STRING"),
    ColumnSpec("EventRootCode", "STRING"),
    ColumnSpec("QuadClass", "INTEGER"),
    ColumnSpec("GoldsteinScale", "FLOAT"),
    ColumnSpec("NumMentions", "INTEGER"),
    ColumnSpec("NumSources", "INTEGER"),
    ColumnSpec("NumArticles", "INTEGER"),
    ColumnSpec("AvgTone", "FLOAT"),
    ColumnSpec("Actor1Geo_Type", "INTEGER"),
    ColumnSpec("Actor1Geo_FullName", "STRING"),
    ColumnSpec("Actor1Geo_CountryCode", "STRING"),
    ColumnSpec("Actor1Geo_ADM1Code", "STRING"),
    ColumnSpec("Actor1Geo_ADM2Code", "STRING"),
    ColumnSpec("Actor1Geo_Lat", "FLOAT"),
    ColumnSpec("Actor1Geo_Long", "FLOAT"),
    ColumnSpec("Actor1Geo_FeatureID", "STRING"),
    ColumnSpec("Actor2Geo_Type", "INTEGER"),
    ColumnSpec("Actor2Geo_FullName", "STRING"),
    ColumnSpec("Actor2Geo_CountryCode", "STRING"),
    ColumnSpec("Actor2Geo_ADM1Code", "STRING"),
    ColumnSpec("Actor2Geo_ADM2Code", "STRING"),
    ColumnSpec("Actor2Geo_Lat", "FLOAT"),
    ColumnSpec("Actor2Geo_Long", "FLOAT"),
    ColumnSpec("Actor2Geo_FeatureID", "STRING"),
    ColumnSpec("ActionGeo_Type", "INTEGER"),
    ColumnSpec("ActionGeo_FullName", "STRING"),
    ColumnSpec("ActionGeo_CountryCode", "STRING"),
    ColumnSpec("ActionGeo_ADM1Code", "STRING"),
    ColumnSpec("ActionGeo_ADM2Code", "STRING"),
    ColumnSpec("ActionGeo_Lat", "FLOAT"),
    ColumnSpec("ActionGeo_Long", "FLOAT"),
    ColumnSpec("ActionGeo_FeatureID", "STRING"),
    ColumnSpec("DATEADDED", "INTEGER"),
    ColumnSpec("SOURCEURL", "STRING"),
)

MENTIONS_COLUMNS: Tuple[ColumnSpec, ...] = (
    ColumnSpec("GLOBALEVENTID", "INTEGER"),
    ColumnSpec("EventTimeDate", "INTEGER"),
    ColumnSpec("MentionTimeDate", "INTEGER"),
    ColumnSpec("MentionType", "INTEGER"),
    ColumnSpec("MentionSourceName", "STRING"),
    ColumnSpec("MentionIdentifier", "STRING"),
    ColumnSpec("SentenceID", "INTEGER"),
    ColumnSpec("Actor1CharOffset", "INTEGER"),
    ColumnSpec("Actor2CharOffset", "INTEGER"),
    ColumnSpec("ActionCharOffset", "INTEGER"),
    ColumnSpec("InRawText", "INTEGER"),
    ColumnSpec("Confidence", "INTEGER"),
    ColumnSpec("MentionDocLen", "INTEGER"),
    ColumnSpec("MentionDocTone", "FLOAT"),
    ColumnSpec("MentionDocTranslationInfo", "STRING"),
    ColumnSpec("Extras", "STRING"),
)

GKG_COLUMNS: Tuple[ColumnSpec, ...] = (
    ColumnSpec("GKGRECORDID", "STRING"),
    ColumnSpec("DATE", "INTEGER"),
    ColumnSpec("SourceCollectionIdentifier", "INTEGER"),
    ColumnSpec("SourceCommonName", "STRING"),
    ColumnSpec("DocumentIdentifier", "STRING"),
    ColumnSpec("Counts", "STRING"),
    ColumnSpec("V2Counts", "STRING"),
    ColumnSpec("Themes", "STRING"),
    ColumnSpec("V2Themes", "STRING"),
    ColumnSpec("Locations", "STRING"),
    ColumnSpec("V2Locations", "STRING"),
    ColumnSpec("Persons", "STRING"),
    ColumnSpec("V2Persons", "STRING"),
    ColumnSpec("Organizations", "STRING"),
    ColumnSpec("V2Organizations", "STRING"),
    ColumnSpec("V2Tone", "STRING"),
    ColumnSpec("Dates", "STRING"),
    ColumnSpec("GCAM", "STRING"),
    ColumnSpec("SharingImage", "STRING"),
    ColumnSpec("RelatedImages", "STRING"),
    ColumnSpec("SocialImageEmbeds", "STRING"),
    ColumnSpec("SocialVideoEmbeds", "STRING"),
    ColumnSpec("Quotations", "STRING"),
    ColumnSpec("AllNames", "STRING"),
    ColumnSpec("Amounts", "STRING"),
    ColumnSpec("TranslationInfo", "STRING"),
    ColumnSpec("Extras", "STRING"),
)

TABLE_COLUMNS: Dict[TableKind, Tuple[ColumnSpec, ...]] = {
    "events": EVENTS_COLUMNS,
    "mentions": MENTIONS_COLUMNS,
    "gkg": GKG_COLUMNS,
}

TABLE_ENV_KEYS: Dict[TableKind, str] = {
    "events": "SNOWFLAKE_TABLE_EVENTS",
    "mentions": "SNOWFLAKE_TABLE_MENTIONS",
    "gkg": "SNOWFLAKE_TABLE_GKG",
}

PIPE_ENV_KEYS: Dict[TableKind, str] = {
    "events": "SNOWFLAKE_PIPE_EVENTS",
    "mentions": "SNOWFLAKE_PIPE_MENTIONS",
    "gkg": "SNOWFLAKE_PIPE_GKG",
}

DEFAULT_TABLE_NAMES: Dict[TableKind, str] = {
    "events": "EVENTS",
    "mentions": "EVENTMENTIONS",
    "gkg": "GKG",
}

# GDELT filename suffixes, used both to detect table kind from a masterfilelist
# URL and to construct predictable URLs for a given 15-minute timestamp.
FILENAME_SUFFIXES: Dict[TableKind, str] = {
    "events": "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "gkg": "gkg.csv.zip",
}

# Ingest lineage columns appended to every table (not present in the raw GDELT
# files). "_GDELT_TIMESTAMP" is the 15-minute window the row came from;
# "client_ts_ms" mirrors the ROW_TIMESTAMP convention used by ../streaming_gdelt
# so ingest-latency queries work the same way against either loader's tables.
LINEAGE_COLUMNS: Tuple[ColumnSpec, ...] = (
    ColumnSpec("_GDELT_TIMESTAMP", "STRING"),
    ColumnSpec("client_ts_ms", "INTEGER"),
)


def column_names(table: TableKind) -> List[str]:
    return [col.name for col in TABLE_COLUMNS[table]]


def snowflake_type(kind: ColumnKind) -> str:
    if kind == "INTEGER":
        return "NUMBER(38, 0)"
    if kind == "FLOAT":
        return "FLOAT"
    return "VARCHAR"


def render_table_ddl(table: TableKind, database: str, schema: str, table_name: str) -> str:
    lines = [f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} ("]
    all_cols = list(TABLE_COLUMNS[table]) + list(LINEAGE_COLUMNS)
    for idx, col in enumerate(all_cols):
        comma = "," if idx < len(all_cols) - 1 else ""
        lines.append(f'    "{col.name}" {snowflake_type(col.kind)}{comma}')
    lines.append(")")
    lines.append("ROW_TIMESTAMP = TRUE;")
    return "\n".join(lines)


def render_pipe_ddl(
    table: TableKind, database: str, schema: str, table_name: str, pipe_name: str
) -> str:
    return f"""CREATE PIPE IF NOT EXISTS {database}.{schema}.{pipe_name} AS
COPY INTO {database}.{schema}.{table_name}
FROM TABLE (
    DATA_SOURCE (
        TYPE => 'STREAMING'
    )
)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;"""


def render_watermark_table_ddl(database: str, schema: str, table_name: str) -> str:
    """Streaming sink for watermark rows. Progress is the channel offset token."""
    return f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
    "LAST_TIMESTAMP" VARCHAR(14)
);"""


def render_watermark_pipe_ddl(
    database: str, schema: str, table_name: str, pipe_name: str
) -> str:
    return f"""CREATE PIPE IF NOT EXISTS {database}.{schema}.{pipe_name} AS
COPY INTO {database}.{schema}.{table_name}
FROM TABLE (
    DATA_SOURCE (
        TYPE => 'STREAMING'
    )
)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;"""
