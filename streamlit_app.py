#! /usr/bin/env python
import pandas
import streamlit

streamlit.title('My Parents\' New Healthy Diner')

streamlit.header('Breakfast Favourites')
streamlit.text('🥣 Omega 3 & Blueberry Oatmeal')
streamlit.text('🥗 Kale, Spinach & Rocket Smoothie')
streamlit.text('🐔 Hard-Boiled Free-Range Egg')
streamlit.text('🥑🍞 Avocado Toast')
streamlit.header('🍌🥭 Build Your Own Fruit Smoothie 🥝🍇')

# Fetch reference data from aws s3
my_fruit_list = pandas.read_csv('https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt')
my_fruit_list.set_index('Fruit')

# Render ingredients table + picker
streamlit.multiselect('Piiick some fruits:', list(my_fruit_list.index))
streamlit.dataframe(my_fruit_list)