import pandas as pd
import pyodbc
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

def fetch_data_from_sql():
    try:
        conn_str = (
            "Driver={SQL Server};"
            "Server=LAPTOP-JIKK1725;"
            "Database=PortfolioProject_MarketingAnalytics;"
            "Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        query = """
                SELECT
	                ReviewID,
	                CustomerID,
	                ProductID,
                    ReviewDate,
                    Rating,
                    REPLACE(ReviewText, '  ', ' ') AS ReviewText
                FROM
                    dbo.customer_reviews
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()

def calculate_sentiment(review):
    sia = SentimentIntensityAnalyzer()
    # Get the sentiment scores for the review text
    sentiment = sia.polarity_scores(review)
    # Return the compound score, which is a normalized score between -1 (most negative) and 1 (most positive)
    return sentiment['compound']

# Categorize sentiment based on both the sentiment score and the review rating
def categorize_sentiment(score, rating):
    if score > 0.05: # Positive sentiment score
        if rating >= 4:
            return 'Positive'
        elif rating == 3:
            return 'Mixed Positive'
        else:
            return 'Mixed Negative'
    elif score < -0.05: # Negative sentiment score
        if rating <= 2:
            return 'Negative'
        elif rating == 3:
            return 'Mixed Negative'
        else:
            return 'Mixed Positive'
    else: # Neutral sentiment score
        if rating >= 4:
            return 'Positive'
        elif rating <= 2:
            return 'Negative'
        else:
            return 'Neutral'

# Bucket sentiment scores into text range        
def sentiment_bucket(score):
    if score >= 0.5:
        return '0.5 to 1.0'
    elif 0.0 <= score < 0.5:
        return '0.0 to 0.49'
    elif -0.5 <= score < 0.0:
        return '-0.49 to 0.0'
    else:
        return '-1.0 to -0.5'

customer_reviews_df = fetch_data_from_sql()

if customer_reviews_df is not None:
    customer_reviews_df['SentimentScore'] = customer_reviews_df['ReviewText'].apply(calculate_sentiment)
    customer_reviews_df['SentimentCategory'] = customer_reviews_df.apply(
        lambda row: categorize_sentiment(row['SentimentScore'], row['Rating']), axis=1
    )
    customer_reviews_df['SentimentBucket'] = customer_reviews_df['SentimentScore'].apply(sentiment_bucket)
    customer_reviews_df.to_csv('..\\data\\fact_customer_reviews_with_sentiment.csv', index=False)
    print("Script executed successfully.")