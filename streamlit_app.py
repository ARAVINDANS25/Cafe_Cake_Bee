import streamlit as st
from snowflake.snowpark.functions import col

st.title("🍏🍇🍉 Fruit Selection 🍊🍓🥝")
st.write(
    """Welcome to the Fruit Selection! 
    Choose your favorite fruits and let's create a delicious fruit salad or smoothie! 🥗🍹"""
)

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your smoothie will be: ", name_on_order)


# Get active Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Execute SQL query to fetch data from Snowflake table
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME')) 



ingredients_list = st.multiselect(
    'Choose the ingredients.',
    my_dataframe,
    max_selections=5
)
if ingredients_list:
    ingredients_str = ' '.join(ingredients_list)
    st.write(ingredients_str)

    # Construct the SQL INSERT statement
    my_insert_stmt = """
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{}', '{}')
    """.format(ingredients_str, name_on_order)

    time_to_insert = st.button("Submit Order")

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
