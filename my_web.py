import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data
try:
    df = pd.read_csv('getted_data.csv')
except FileNotFoundError:
    st.error("The file 'getted_data.csv' was not found.")
    st.stop()

st.title('Emotion Visualization')

# Emotion Distribution
st.header('Emotion Distribution')
emotion_counts = df['Emotion'].value_counts()

# Pie chart of emotions in the first column
fig, ax = plt.subplots()
emotion_counts.plot(kind='pie', autopct='%1.1f%%', ax=ax)
ax.set_ylabel('')
st.pyplot(fig)

# Age distribution histogram in the second column
fig, ax = plt.subplots()
sns.histplot(df['Age'], bins=10, kde=True, ax=ax)
ax.set_title('Age Distribution of Respondents')
st.pyplot(fig)

# Gender Distribution
st.header('Gender Distribution')
gender_counts = df['Gender'].value_counts()

fig, ax = plt.subplots()
gender_counts.plot(kind='bar', ax=ax, color=['lightblue', 'lightgreen'])
ax.set_title('Gender Distribution')
ax.set_ylabel('Count')
ax.set_xlabel('Gender')
st.pyplot(fig)
