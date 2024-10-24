import streamlit as st
import openai
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
from sqlalchemy import create_engine

openai.api_key = ""

engine = create_engine("postgresql://clever:clever@postgres_clever:5432/clever")

# Functions
def generate_personalized_content(user_options, user_recommendations):
    # Prepare the system prompt
    system_prompt = """
    Create personalized content for real estate customers using the provided JSON data about their preferences and recommendations.

    Your output should consider customer needs, preferences, and any other relevant information provided in the JSON to generate content that feels tailored to the individual customer.

    You should convince customers to buy or sell with Clever Real Estate.

    # Steps

    1. **Analyze JSON Data**: Carefully examine the JSON structure to understand the customer's best company options.
    2. **Content Creation**: Develop personalized content that directly addresses the customer's specific needs and desires.
    3. **Review for Personalization**: Verify that the content uniquely addresses the customer, ensuring it feels personal and directly applicable to them.
    4. **Use markdown**: Your answer must be using markdown

    # Output Format
    - A text about Clever benefits
    - A table with the recommended companies
    - Make a section to show user reviews
    - Ensure the tone is engaging and relevant to real estate customers.

    # About Clever Real Estate
    - The best rates in real estate: These agents have agreed to offer Clever customers our special rate: just a 1.5% listing fee
    - Buy: We connect you with the best buyer’s agents nationwide. Qualifying buyers get cash back after closing. In eligible states, get cash back after closing to go towards moving costs, home improvements, new furniture — anything you want!
    - Sell: Sell with a top agent for just a 1.5% listing fee. Get matched in minutes. Clever sellers get offers 2.8x faster than the national average.

    # Notes

    - Consider edge cases where the JSON might contain incomplete or unusual requests and address these appropriately.
    - Maintain a professional yet friendly tone suitable for the real estate industry.
    - Always tailor the content to make the customer feel individually valued and understood.
    - In the end of the content suggest them to acess the website https://listwithclever.com to talk with a specialist
    """

    # Prepare the user message
    user_message = f"```{user_options}``` and ```{user_recommendations}```"
    # Call the OpenAI API
    try:
        response = openai.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            max_tokens=1000,
            n=1,
            stop=None,
            temperature=0.7,
        )
        # Extract the assistant's reply
        content = response.to_dict().get("choices")[0].get("message").get("content")
        return content
    except Exception as e:
        st.error(f"An error occurred while generating content: {e}")
        return "Sorry, we couldn't generate personalized content at this time."



def get_coordinates(address_or_postal_code):
    geolocator = Nominatim(user_agent="your_app_name")  # Replace with your app name
    try:
        location = geolocator.geocode(address_or_postal_code, timeout=10)
        if location:
            return (location.latitude, location.longitude)
        else:
            st.error("No coordinates found for the provided address or postal code.")
            return None
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as e:
        st.error(f"Geocoding error: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def calculate_distances(df, coord_tuple):
    lat1, lon1 = coord_tuple
    df['distance'] = df.apply(lambda row: haversine_distance(lat1, lon1, row['latitude'], row['longitude']), axis=1)
    return df

# Streamlit app
st.title("Real Estate Service Finder")

# 1. Collect the user's name
user_name = st.text_input("What's your name?")

# 2. Ask about the desired action with options
possible_actions = ["buy", "sell", "move"]
action = st.selectbox("Are you looking to buy a house, sell a house, or move?", possible_actions)

types_by_action = {
    "buy": [
        'Real estate agency',
        'Real estate consultant',
        'Real estate agent',
        'Property investment'
    ],
    "sell": [
        'Property management company',
        'Real estate agency',
        'Real estate consultant',
        'Real estate agent'
    ],
    "move": [
        'Moving and storage service',
        'Mover'
    ]
}

# 3. Collect the user's address
address = st.text_input("What is your address? You can also provide just your postal code.")

if user_name and action and address:
    st.write(f"Hello, {user_name}!")

    # Get the action types
    action_types = types_by_action.get(action)
    type_items = ', '.join(f"'{t}'" for t in action_types)
    type_filter = f"type in ({type_items})"

    # Get coordinates
    coordinates = get_coordinates(address)
    if coordinates:
        # Query the database
        query = f"""
        WITH reviews_score AS (
            SELECT google_id, AVG(user_review_score) AS review_score
            FROM curated_score_customer_reviews_google
            GROUP BY google_id
        )
        SELECT 
            dc.*, 
            fc.company_profile_score AS company_score,
            rs.review_score
        FROM 
            curated_dim_company_profiles_google_maps dc
        INNER JOIN
            curated_score_company_profiles_google_maps fc
            ON dc.google_id = fc.google_id
        LEFT JOIN
            reviews_score rs
            ON rs.google_id = fc.google_id
        WHERE {type_filter}
        ORDER BY company_profile_score DESC
        """
        df = pd.read_sql(query, engine)

        # Calculate distances and sort
        df = calculate_distances(df, coordinates)
        df_best = df.sort_values(["distance", "company_score"], ascending=[True, False]).head(5).reset_index(drop=True)

        user_options = {
            'user_name': user_name,
            'action': action,
            'address': address,
            'coordinates': coordinates
        }

        user_recommendations = {
            'best_companies': df_best[['name', 'city', 'type', 'rating', 'reviews', 'distance']].to_dict(orient='records'),
        }

        # Optionally, display reviews
        company_ids = df_best.google_id.tolist()
        company_ids_str = ', '.join(f"'{id}'" for id in company_ids)

        review_query = f"""
        SELECT
            fc.review_text
        FROM 
            curated_fact_customer_reviews_google fc
        INNER JOIN
            curated_score_customer_reviews_google sc
        ON
            fc.review_id = sc.review_id
        WHERE 
            fc.google_id IN ({company_ids_str})
        ORDER BY
            user_review_score DESC
        LIMIT 5
        """
        reviews_df = pd.read_sql(review_query, engine)
        user_recommendations['best_reviews'] = reviews_df['review_text'].tolist()

        # Generate personalized content
        with st.spinner('Generating personalized content...'):
            personalized_content = generate_personalized_content(user_options, user_recommendations)

        # Display the content in the app
        st.markdown(personalized_content, unsafe_allow_html=True)
    else:
        st.error("Unable to retrieve coordinates. Please check your address or postal code.")
