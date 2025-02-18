# Music Recommendation System

## Overview
The Music Recommendation System is a data-driven application designed to provide personalized music recommendations using collaborative filtering techniques. The project integrates **Apache Kafka, Apache Spark, MongoDB, and Machine Learning algorithms** to analyze user preferences and suggest relevant songs.

## Features
- **Personalized Song Recommendations** using collaborative filtering.
- **Big Data Processing** with Apache Spark.
- **Real-time Data Streaming** with Apache Kafka.
- **Data Storage** using MongoDB.
- **Machine Learning Algorithms** for clustering and recommendation.
- **Visualization** using Tableau.
- **User Interface** with Streamlit.

## Dataset
The project utilizes the **Spotify Song Dataset**, which contains metadata and audio features for a large collection of songs.

### Key Attributes:
- **Song Name**
- **Artist Name**
- **Popularity**
- **Danceability**
- **Energy**
- **Loudness**
- **Tempo**
- **Duration** (ms) and more...

## Tech Stack
### Backend Technologies:
- **Python** (Data Analysis & ML Modeling)
- **Apache Kafka** (Real-time Data Processing)
- **Apache Spark** (Big Data Processing)
- **MongoDB** (NoSQL Database for Storage)

### Machine Learning:
- **K-Means Clustering** for grouping similar songs.
- **Collaborative Filtering** for personalized recommendations.

### Visualization:
- **Tableau** for data analysis and visualization.
- **Streamlit** for UI development and interactive recommendations.

### Software:
- **OS:** Windows 10 or higher / Linux / macOS
- **Python 3.x** with the following libraries:
  - NumPy
  - pandas
  - scikit-learn
  - Matplotlib & Seaborn
  - PyMongo
- **MongoDB** (for storing song data)
- **Apache Kafka** (for real-time data streaming)
- **Tableau** (for visualization)

## Project Architecture
1. **Data Collection & Storage**: The dataset is stored in MongoDB.
2. **Preprocessing & Feature Extraction**: Data is cleaned and features are extracted.
3. **Clustering & Model Training**: K-Means clustering groups similar songs.
4. **Recommendation Generation**: Collaborative filtering provides song suggestions.
5. **Visualization & UI**: Recommendations are displayed using Streamlit & Tableau.

## Future Scope
- **Real-Time Recommendation System** with live user feedback.
- **Multi-modal Data Integration** (Lyrics, Audio Features, Album Covers).
- **Enhanced ML Models** for better song matching.

## Contributors
- **Shubham Bane**
- **Ritik Varma**
- **Mohit Bhagwat**

## References
- [MongoDB](https://www.mongodb.com/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Scikit-Learn](https://scikit-learn.org/)

