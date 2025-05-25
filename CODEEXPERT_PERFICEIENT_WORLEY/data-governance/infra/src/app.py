import sys
import streamlit as st
import pandas as pd
import json
from helpers import dynamodb,utilities,lakeformation


if "visibility" not in st.session_state:
    st.session_state.visibility = "visible"
    st.session_state.disabled = False

print(sys.path)

dyno = dynamodb()
utilities = utilities()
logger = utilities.get_logger()
settings = utilities.get_settings()

table_def = []

items = dyno.get_all_db_items(settings["database_permissions_store"])

for i in items:
    table_obj = {
        "Database Name" : i["database_name"],
        "Domain" : i["domain_name"]
    }

    table_def.append(table_obj)

logger.info(json.dumps(items))
logger.info(type(items))

col1, col2 = st.columns(2)

with col1:
    option = st.selectbox(
        label ="Select Database",
        options = table_def,
        index=None
        )


with col2:
    for i in items:
        if i["database_name"] == option:
            df = pd.read_json(json.dumps(i))
            st.table(df,visible=True)
            logger.info(i)
            st.write(i)

df = pd.read_json(json.dumps(table_def))
# Create a connection:
# conn = st.connection(
#     "s3", type=DynamoDBConnection, api_type="pandas"
# )

# Get all items in the table:
# st.write("All items in the table:")
# st.table(df)

# st.write("Json def")
# st.write(items)

# Get a single item by key:
# item = conn.get_item("first_item")
# st.write(item)

# # Put an item in the table:
# conn.put_item(
#     "new_item",
#     {
#         "text": "This item was put from streamlit!",
#         "metadata": {"source": "mrtj"},
#    }
# )

# # Modify an existing item:
# conn.modify_item(
#     "new_item",
#     {
#         "text": "This item was put and modified from streamlit!",
#         "metadata": None,
#         "new_field": "This is a newly added field"
#     }
# )

# # Delete an item from the table:
# conn.del_item("new_item")