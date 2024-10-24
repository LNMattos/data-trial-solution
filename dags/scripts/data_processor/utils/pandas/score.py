import pandas as pd
import numpy as np
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

sia = SentimentIntensityAnalyzer()

### 4. Calculate Adjectives Sentiment Score (S₃)
def calculate_adjectives_sentiment(adjectives):
    adjectives_list = adjectives.split(",")
    if not adjectives_list or not isinstance(adjectives_list, list):
        return 0  # Neutral if no adjectives
    sentiment_scores = []
    for adj in adjectives_list:
        # Get sentiment score for each adjective
        score = sia.polarity_scores(adj)['compound']
        if score >= 0.05:
            score = 1
        elif score <= -0.05:
            score = -1
        if score!=0:
            sentiment_scores.append(score)
    # Return the average sentiment score
    return np.mean(sentiment_scores)

def calculate_user_review_score(df, lambda_decay=0.001, weights=None, drop_intermediate_cols=True, drop_non_used_cols=False):
    """
    Calculates a composite score for customer reviews.
    
    Parameters:
    - df: pandas DataFrame containing the reviews.
    - lambda_decay: float, decay constant for the recency weight.
    - weights: dict, weights for each feature.
    
    Returns:
    - df: pandas DataFrame with additional columns for the scores.
    """
    # Set default weights if none are provided
    if weights is None:
        weights = {
            'review_rating': 0.6,
            'sentiment': 0.3,
            'adjectives_sentiment': 0.1, # Not used 
            'review_likes': 0 # Not used 
        }
    
    # Ensure weights sum to 1
    total_weight = sum(weights.values())
    weights = {k: v / total_weight for k, v in weights.items()}

    feature_cols = ["google_id", "review_id", "review_rating", "review_likes", "sentiment", "adjectives", "review_timestamp"]
    df = df.copy()
    if drop_non_used_cols:
        df = df[feature_cols]
        
    
    ### 1. Normalize Review Rating (S₁)
    df['S1'] = (df['review_rating'] - 1) / 4  # Scales ratings from 1-5 to 0-1
    
    ### 2. Normalize Review Likes (L)
    # Apply logarithmic transformation
    df['log_likes'] = np.log(df['review_likes'] + 1)
    min_log_likes = df['log_likes'].min()
    max_log_likes = df['log_likes'].max()
    
    # Min-max normalization
    df['L'] = (df['log_likes'] - min_log_likes) / (max_log_likes - min_log_likes)
    
    ### 3. Map Sentiment to Numerical Values (S₂)
    sentiment_mapping = {'Negative': 0, 'Neutral': 0.5, 'Positive': 1}
    df['S2'] = df['sentiment'].apply(lambda val: sentiment_mapping.get(val, 0.5))
    
    # Handle any unmapped sentiments
    df['S2'] = df['S2'].fillna(0)
    
    df['S3'] = df['adjectives'].apply(calculate_adjectives_sentiment)
    
    ### 5. Calculate Recency Weight (W)
    df['review_timestamp'] = pd.to_datetime(df['review_timestamp'])
    ref_date = df['review_timestamp'].max()
    
    # Calculate the time difference in days
    df['delta_t'] = (ref_date - df['review_timestamp']).dt.days
    
    # Apply exponential decay function
    df['W'] = np.exp(-lambda_decay * df['delta_t'])
    
    ### 6. Calculate Composite Score
    df['user_review_score'] = round(df['W'] * (
        weights['review_rating'] * df['S1'] +
        weights['sentiment'] * df['S2'] +
        weights['adjectives_sentiment'] * df['S3'] +
        weights['review_likes'] * df['L']
    ),2)
    
    if drop_intermediate_cols:
        df = df.drop(columns=['S1', 'S2', 'S3', 'L', 'log_likes', 'delta_t', 'W'])
        
    df.insert(0, "process_timestamp", datetime.now())
    
    return df


def calculate_company_score(df, weights=None, drop_intermediate_cols=True, drop_non_used_cols=False):
    """
    Calculates a composite score for companies based on rating, reviews count, and photos count.

    Parameters:
    - df: pandas DataFrame containing the company profiles.
    - weights: dict, weights for each feature.

    Returns:
    - df: pandas DataFrame with an additional 'Company_Score' column.
    """

    # Set default weights if none are provided
    if weights is None:
        weights = {
            'rating': 0.6,
            'reviews': 0.3,
            'photos_count': 0.1
        }

    # Ensure weights sum to 1
    total_weight = sum(weights.values())
    weights = {k: v / total_weight for k, v in weights.items()}

    feature_cols = ["google_id", "rating", "reviews", "photos_count"]
    df = df.copy()
    if drop_non_used_cols:
        df = df[feature_cols]

    ### 1. Normalize Rating (S1)
    df['S1'] = (df['rating'] - 1) / 4  # Scale ratings from 1-5 to 0-1

    ### 2. Normalize Reviews Count (S2)
    # Apply logarithmic transformation
    df['log_reviews'] = np.log(df['reviews'] + 1)
    min_log_reviews = df['log_reviews'].min()
    max_log_reviews = df['log_reviews'].max()

    # Avoid division by zero if max and min are equal
    if max_log_reviews == min_log_reviews:
        df['S2'] = 0.0
    else:
        # Min-max normalization
        df['S2'] = (df['log_reviews'] - min_log_reviews) / (max_log_reviews - min_log_reviews)

    ### 3. Normalize Photos Count (S3)
    # Apply logarithmic transformation
    df['log_photos'] = np.log(df['photos_count'] + 1)
    min_log_photos = df['log_photos'].min()
    max_log_photos = df['log_photos'].max()

    # Avoid division by zero if max and min are equal
    if max_log_photos == min_log_photos:
        df['S3'] = 0.0
    else:
        # Min-max normalization
        df['S3'] = (df['log_photos'] - min_log_photos) / (max_log_photos - min_log_photos)

    ### 4. Compute the Composite Score
    df['company_profile_score'] = (
        weights['rating'] * df['S1'] +
        weights['reviews'] * df['S2'] +
        weights['photos_count'] * df['S3']
    )

    if drop_intermediate_cols:
        df = df.drop(columns=['S1', 'S2', 'S3', 'log_reviews', 'log_photos'])
        
    df.insert(0, "process_timestamp", datetime.now())
    
    return df
