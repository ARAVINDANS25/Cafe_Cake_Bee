import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_star: Pending Smoothie Orders :cup_with_star:")
st.write("Here are the pending smoothie orders:")

# Get active Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Execute SQL query to fetch pending orders from Snowflake table
pending_orders = session.table("smoothies.public.orders").filter(col('ORDER_FILLED') == False).to_pandas()

if pending_orders.empty:  # Check if the DataFrame is empty
    st.success("There are no pending orders right now :thumbsup:")
else:
    # Display pending orders using Streamlit
    edited_df = st.data_editor(pending_orders)

    # Submit button to mark orders as delivered
    submitted = st.button('Submit')
    if submitted:
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(edited_df)

        try:    
            og_dataset.merge(edited_dataset
                     , (og_dataset['order_uid'] == edited_dataset['order_uid'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                    )
            st.success("Order Delivered :thumbsup:")
        except:
            st.write('Something went wrong')    
