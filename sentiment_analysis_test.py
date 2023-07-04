import apache_beam as beam
from textblob import TextBlob

def sentiment_analysis(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return 'positive'
    elif sentiment < 0:
        return 'negative'
    else:
        return 'neutral'

class SentimentAnalysis(beam.DoFn):
    def process(self, element):
        text = element.strip()
        sentiment = sentiment_analysis(text)
        yield (sentiment, text)

def run_sentiment_analysis(input_text):
    with beam.Pipeline() as p:
        # Read the input text from a list
        text_data = p | beam.Create(input_text)

        # Apply sentiment analysis to each text element
        sentiment_scores = text_data | beam.ParDo(SentimentAnalysis())

        # Write the sentiment scores to a file
        sentiment_scores | beam.io.WriteToText('sentiment_scores')

if __name__ == '__main__':
    input_text = [
        'I love Java!',
        'I feel sad.',
        'The API is not working!',
        'Python is horrible!',
        'I got an error message.'
    ]
    run_sentiment_analysis(input_text)
