import streamlit as st
import pandas as pd

st.title("Autonomous Data Ecosystem Dashboard")

df = pd.read_csv("artifacts/" + sorted(os.listdir("artifacts"))[-1])

st.metric("Total Rows", len(df))
st.metric("Avg Salary", int(df["salary"].mean()))

st.subheader("Salary by Department")
st.bar_chart(df.groupby("department")["salary"].mean())

st.subheader("Headcount")
st.bar_chart(df["department"].value_counts())
