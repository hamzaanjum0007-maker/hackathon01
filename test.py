import requests
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import plotly.express as px

DB_NAME = "crypto_data.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT, symbol TEXT, name TEXT,
            current_price REAL, market_cap REAL,
            price_change_24h REAL, price_change_percentage_24h REAL,
            total_volume REAL, last_updated TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ Database ready!")

def run_etl():
    st.info("📥 Fetching data from API...")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 20, "page": 1}
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        
        if resp.status_code != 200:
            st.error(f"❌ API Error: {resp.status_code}")
            return False
            
        data = resp.json()
        
        if not data:
            st.warning("⚠️ No data returned!")
            return False
        
        conn = sqlite3.connect(DB_NAME)
        timestamp = datetime.now()
        
        for coin in data:
            conn.execute('''INSERT INTO crypto_prices 
                (coin_id, symbol, name, current_price, market_cap, 
                 price_change_24h, price_change_percentage_24h, 
                 total_volume, last_updated, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (coin.get("id"), coin.get("symbol","").upper(), coin.get("name"),
                 coin.get("current_price",0), coin.get("market_cap",0),
                 coin.get("price_change_24h",0), coin.get("price_change_percentage_24h",0),
                 coin.get("total_volume",0), coin.get("last_updated"), timestamp))
        
        conn.commit()
        conn.close()
        st.success(f"✅ Saved {len(data)} coins!")
        return True
        
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        return False

def get_data():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query('''
        SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY created_at DESC) as rn 
        FROM crypto_prices) WHERE rn = 1 ORDER BY market_cap DESC LIMIT 20
    ''', conn)
    conn.close()
    return df

def main():
    st.set_page_config(page_title="Crypto Analytics", page_icon="₿", layout="wide")
    st.title("₿ Cryptocurrency Analytics Platform")
    st.markdown("---")
    
    # Refresh Button
    if st.button("🔄 Fetch Latest Data"):
        run_etl()
        st.rerun()
    
    # Get Data
    df = get_data()
    
    # Check if empty
    if df.empty:
        st.warning("⚠️ No data found! Click 'Fetch Latest Data' button above.")
        return
    
    # Show Data
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Market Cap", f"${df['market_cap'].sum()/1e12:.2f}T")
    c2.metric("24h Volume", f"${df['total_volume'].sum()/1e9:.2f}B")
    c3.metric("Coins", len(df))
    c4.metric("Top Coin", df.iloc[0]['name'], f"${df.iloc[0]['current_price']:,.2f}")
    
    st.markdown("---")
    
    g1, g2 = st.columns(2)
    with g1:
        st.markdown("### 🟢 Top Gainers")
        st.dataframe(df.nlargest(5, 'price_change_percentage_24h')[['name','symbol','current_price','price_change_percentage_24h']].style.format({'current_price':'${:.2f}', 'price_change_percentage_24h':'{:+.2f}%'}))
    with g2:
        st.markdown("### 🔴 Top Losers")
        st.dataframe(df.nsmallest(5, 'price_change_percentage_24h')[['name','symbol','current_price','price_change_percentage_24h']].style.format({'current_price':'${:.2f}', 'price_change_percentage_24h':'{:+.2f}%'}))
    
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📊 Market Cap Pie", "📈 Price Bar"])
    with tab1:
        st.plotly_chart(px.pie(df.head(10), values='market_cap', names='name', title='Market Cap Distribution'), use_container_width=True)
    with tab2:
        st.plotly_chart(px.bar(df.head(10), x='name', y='current_price', color='price_change_percentage_24h', color_continuous_scale='RdYlGn', title='Price Comparison'), use_container_width=True)
    
    st.markdown("---")
    st.subheader("💰 All Cryptocurrency Data")
    st.dataframe(df[['name','symbol','current_price','market_cap','price_change_percentage_24h','total_volume']].style.format({'current_price':'${:,.2f}', 'market_cap':'${:,.0f}', 'price_change_percentage_24h':'{:+.2f}%', 'total_volume':'${:,.0f}'}), use_container_width=True)
    
    csv = df.to_csv(index=False)
    st.download_button("📥 Download CSV", csv, "crypto.csv", "text/csv")
    st.caption(f"Last Updated: {datetime.now()}")

if __name__ == "__main__":
    init_db()
    df = get_data()
    if df.empty:
        run_etl()
    main()