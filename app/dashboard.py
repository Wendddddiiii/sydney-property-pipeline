import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import Config

st.set_page_config(
    page_title = "Sydney Property Market Dashboard",
    page_icon = "🏡",
    layout="wide"
)

#@st.cache_resource
#def get_db_connection():
#    config = Config()
#    conn = psycopg2.connect(
#        host = config.DB_HOST,
#        port = config.DB_PORT,
#        database = config.DB_NAME,
#        user = config.DB_USER,
#        password = config.DB_PASSWORD
#    )
#    return conn 

@st.cache_data
def load_data():
    """Load data from CSV file"""
    try:
        # Use relative path for deployment
        df = pd.read_csv('data/processed/properties_processed_latest.csv')
        
        # Convert date column if needed
        if 'date_sold' in df.columns:
            df['date_sold'] = pd.to_datetime(df['date_sold'], errors='coerce')
        
        return df
    except FileNotFoundError:
        st.error("Data file not found. Please check the data directory.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def main():
    st.title("🏡 Sydney Property Market Analytics Dashboard")
    st.markdown("---")
    with st.spinner("Loading data......"):
        df = load_data()

    st.sidebar.header("Filters")

    min_price = int(df['price'].min())
    max_price = int(df['price'].max())
    price_range = st.sidebar.slider(
        "Price Range ($)",
        min_price,
        max_price,
        (min_price, max_price),
        step = 50000
    )

    property_types = ['All'] + sorted(df['type'].unique().tolist())
    selected_type = st.sidebar.selectbox("Property Type", property_types)

    distance_cats = ['All'] + sorted(df['distance_category'].dropna().unique().tolist())
    selected_distance = st.sidebar.selectbox("Distance from CBD", distance_cats)

    filtered_df = df[(df['price'] >= price_range[0]) & (df['price'] <= price_range[1])]

    if selected_type != 'All':
        filtered_df = filtered_df[filtered_df['type'] == selected_type]
    
    if selected_distance != 'All':
        filtered_df = filtered_df[filtered_df['distance_category'] == selected_distance]

    #KPI Metrics
    st.header("📊 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Properties",
            f"{len(filtered_df):,}",
            delta=f"{len(filtered_df) - len(df):,} filtered"
        )
    
    with col2:
        avg_price = filtered_df['price'].mean()
        st.metric(
            "Average Price",
            f"${avg_price:,.0f}"
        )
    
    with col3:
        avg_sqm = filtered_df['price_per_sqm'].mean()
        st.metric(
            "Avg Price/Sqm",
            f"${avg_sqm:,.0f}" if pd.notnull(avg_sqm) else "N/A"
        )

    with col4:
        num_suburbs = filtered_df['suburb'].nunique()
        st.metric(
            "Suburbs",
            f"{num_suburbs:,}"
        )


    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Price Distribution")
        fig_hist = px.histogram(
            filtered_df,
            x='price',
            nbins=50,
            title='Property Price Distribution',
            labels = {'price': 'Price($)', 'count': 'Number of Properties'}
        )

        fig_hist.update_layout(showlegend = False)
        st.plotly_chart(fig_hist, config={'responsive': True})

    with col2:
        st.subheader("🏘️ Properties by Distance Category")
        distance_counts = filtered_df['distance_category'].value_counts()
        fig_pie = px.pie(
            values = distance_counts.values,
            names=distance_counts.index,
            title = 'Distribution by Distance from CBD'
        )

        st.plotly_chart(fig_pie, config={'responsive': True})

    
    st.subheader("📍 Price vs Distance from CBD")
    fig_scatter = px.scatter(
        filtered_df,
        x='km_from_cbd',
        y='price',
        color='is_house',
        size='num_bed',
        hover_data=['suburb', 'type', 'num_bed', 'num_bath'],
        title='Property Price vs Distance from CBD',
        labels={
            'km_from_cbd':'Distance from CBD(km)',
            'price': 'Price($)',
            'is_House': 'Property Type'
        },
        color_discrete_map={True: '#FF6B6B', False: '#4ECDC4'}
    )
    st.plotly_chart(fig_scatter, config={'responsive': True})

    st.subheader("Top 10 Most Expensive Suburbs")
    suburb_stats = filtered_df.groupby('suburb').agg({
        'price':['count', 'mean', 'max'],
        'km_from_cbd': 'mean'
    }).round(2)

    suburb_stats.columns=['Count', 'Avg Price', 'Max Price', 'Avg Distance (km)']
    suburb_stats = suburb_stats.sort_values('Avg Price', ascending=False)
    suburb_stats['Avg Price']=suburb_stats['Avg Price'].apply(lambda x: f"${x:,.0f}")
    suburb_stats['Max Price']=suburb_stats['Max Price'].apply(lambda x: f"${x:,.0f}")
    st.dataframe(suburb_stats, width='stretch')
    
    # House vs Apartment comparison
    st.subheader("🏠 House vs Apartment Comparison")

    # Check if we have enough data
    if len(filtered_df) > 0 and 'is_house' in filtered_df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            # Average price comparison
            house_apt_avg = filtered_df.groupby('is_house')['price'].mean()
            
            categories = []
            values = []
            colors = []
            
            if False in house_apt_avg.index:
                categories.append('Apartment')
                values.append(house_apt_avg[False])
                colors.append('#4ECDC4')
            
            if True in house_apt_avg.index:
                categories.append('House')
                values.append(house_apt_avg[True])
                colors.append('#FF6B6B')
            
            if len(categories) > 0:
                fig_bar = go.Figure(data=[
                    go.Bar(
                        x=categories,
                        y=values,
                        marker_color=colors,
                        text=[f'${v:,.0f}' for v in values],
                        textposition='outside'
                    )
                ])
                fig_bar.update_layout(
                    title='Average Price: House vs Apartment',
                    yaxis_title='Average Price ($)',
                    showlegend=False
                )
                st.plotly_chart(fig_bar, config={'responsive': True})
            else:
                st.info("No data for this comparison")
        
        with col2:
            # Count comparison
            house_apt_count = filtered_df['is_house'].value_counts()
            
            categories = []
            counts = []
            colors = []
            
            if False in house_apt_count.index:
                categories.append('Apartment')
                counts.append(house_apt_count[False])
                colors.append('#4ECDC4')
            
            if True in house_apt_count.index:
                categories.append('House')
                counts.append(house_apt_count[True])
                colors.append('#FF6B6B')
            
            if len(categories) > 0:
                fig_count = go.Figure(data=[
                    go.Bar(
                        x=categories,
                        y=counts,
                        marker_color=colors,
                        text=counts,
                        textposition='outside'
                    )
                ])
                fig_count.update_layout(
                    title='Property Count: House vs Apartment',
                    yaxis_title='Number of Properties',
                    showlegend=False
                )
                st.plotly_chart(fig_count, config={'responsive': True})
            else:
                st.info("No data for this comparison")
    else:
        st.warning("No properties match the selected filters. Try adjusting the criteria.")

    st.markdown("---")
    st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"Total dataset: {len(df):,} properties")


if __name__ == "__main__":
    main()

