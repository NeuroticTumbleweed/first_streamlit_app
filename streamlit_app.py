#! /usr/bin/env python
import pandas
import requests
import streamlit
import snowflake.connector
from urllib.error import URLError

SNOWFLAKE_CONN = snowflake.connector.connect(**streamlit.secrets["snowflake"])

def main():
    streamlit.title('My Parents\' New Healthy Diner')

    streamlit.header('Breakfast Favourites')
    streamlit.text('🥣 Omega 3 & Blueberry Oatmeal')
    streamlit.text('🥗 Kale, Spinach & Rocket Smoothie')
    streamlit.text('🐔 Hard-Boiled Free-Range Egg')
    streamlit.text('🥑🍞 Avocado Toast')
    streamlit.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')

    # Fetch reference data from aws s3
    my_fruit_list = pandas.read_csv('https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt')
    my_fruit_list = my_fruit_list.set_index('Fruit')

    # Render ingredients table + picker
    fruits_selected = streamlit.multiselect(
        'Pick some fruits:',
        list(my_fruit_list.index),
        ['Avocado', 'Strawberries'],
    )
    fruits_to_show = my_fruit_list.loc[fruits_selected]
    streamlit.dataframe(fruits_to_show)

    streamlit.header('Fruityvice Fruit Advice!')

    try:
        fruit_choice = streamlit.text_input('What fruit would you like information about?')
        if not fruit_choice:
            streamlit.error("Please select a fruit to get information")
        else:
            fruityvice_normalized = get_fruityvice_data(fruit_choice)
            streamlit.dataframe(fruityvice_normalized)
    except URLError as e:
        streamlit.error()

    if streamlit.button('Get Fruit Load List'):
        fruit_list = get_fruit_load_list()
        streamlit.header("The fruit load list contains:")
        streamlit.dataframe(fruit_list)

    add_my_fruit = streamlit.text_input('What fruit would you like to add?')
    if streamlit.button('Add a fruit to the list.'):
        streamlit.text(insert_row_snowflake(add_my_fruit))


def get_fruityvice_data(fruit_choice):
    """Query fruityvice for fruit fruit_choice."""
    fruityvice_response = requests.get(f'https://fruityvice.com/api/fruit/{fruit_choice}')
    fruityvice_normalized = pandas.json_normalize(fruityvice_response.json())
    fruityvice_normalized = fruityvice_normalized.set_index('name')
    return fruityvice_normalized


def get_fruit_load_list():
    """Get the fruit's loaded into the snowflake import table."""
    with SNOWFLAKE_CONN.cursor() as cursor:
        cursor.execute('select * from fruit_load_list')
        return cursor.fetchall()


def insert_row_snowflake(new_fruit):
    with SNOWFLAKE_CONN.cursor() as cursor:
        cursor.execute("insert into fruit_load_list values ('{new_fruit}')", new_fruit)
        return f"Thanks for adding {new_fruit}!"


if __name__ == '__main__':
    main()