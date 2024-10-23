from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('averaged_perceptron_tagger')
nltk.download('averaged_perceptron_tagger_eng')

def classify_sentiment_vader(df, text_column, new_column='sentiment'):
    analyzer = SentimentIntensityAnalyzer()
    
    def get_sentiment(text):
        scores = analyzer.polarity_scores(text)
        compound = scores['compound']
        if compound >= 0.05:
            return 'Positive'
        elif compound <= -0.05:
            return 'Negative'
        else:
            return 'Neutral'
    
    df[new_column] = df[text_column].fillna('').astype(str).apply(get_sentiment)
    return df

def extract_adjectives_nltk(df, text_column, new_column='adjectives'):
    def get_adjectives(text):
        tokens = nltk.word_tokenize(text)
        pos_tags = nltk.pos_tag(tokens)
        adjectives = [word for word, pos in pos_tags if pos.startswith('JJ')]
        return ",".join(list(adjectives))
    
    df[new_column] = df[text_column].fillna('').astype(str).apply(get_adjectives)
    return df
