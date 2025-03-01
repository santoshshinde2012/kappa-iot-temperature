import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
import altair as alt
import time

# Fetch data from Cassandra with filters
@st.cache_data(ttl=300)
def fetch_data(_session, time_range, sensor_ids, temp_filter=None, temp_min=None, temp_max=None, anomaly_filter=None):
    try:
        query = "SELECT sensor_id, timestamp, temperature, is_anomaly FROM temperature_results"
        conditions = []
        params = {}

        # Time Range Filter
        now_unix = time.time()
        time_filters_unix = {
            "Last Hour": now_unix - (1 * 3600),
            "Last 24 Hours": now_unix - (24 * 3600),
            "Last 7 Days": now_unix - (7 * 24 * 3600),
            "Last 30 Days": now_unix - (30 * 24 * 3600),
            "All Time": None
        }
        cutoff_unix = time_filters_unix[time_range]
        if cutoff_unix is not None:
            conditions.append("timestamp >= :cutoff")
            params['cutoff'] = cutoff_unix

        # Sensor ID Filter
        if sensor_ids and "All" not in sensor_ids:
            conditions.append("sensor_id IN :sensor_ids")
            params['sensor_ids'] = tuple(sensor_ids)

        # Temperature Filter (requires index or ALLOW FILTERING)
        if temp_filter == "Above 30°C":
            conditions.append("temperature > 30")
        elif temp_filter == "Below 20°C":
            conditions.append("temperature < 20")
        elif temp_filter == "Custom Range" and temp_min is not None and temp_max is not None:
            conditions.append("temperature >= :temp_min AND temperature <= :temp_max")
            params['temp_min'] = temp_min
            params['temp_max'] = temp_max

        # Anomaly Filter (requires index or ALLOW FILTERING)
        if anomaly_filter == "Anomalies Only":
            conditions.append("is_anomaly = true")
        elif anomaly_filter == "Normal Only":
            conditions.append("is_anomaly = false")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ALLOW FILTERING"  # Use with caution

        prepared = _session.prepare(query)
        rows = _session.execute(prepared, params) if params else _session.execute(query)
        
        df = pd.DataFrame([
            {
                "sensor_id": row.sensor_id,
                "timestamp": row.timestamp,
                "temperature": row.temperature,
                "is_anomaly": row.is_anomaly,
            }
            for row in rows
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True, errors='coerce')
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Get unique sensor IDs
@st.cache_data(ttl=3600)
def get_sensor_ids(_session):
    try:
        query = "SELECT DISTINCT sensor_id FROM temperature_results"
        return sorted([row.sensor_id for row in _session.execute(query)])
    except Exception as e:
        st.error(f"Error fetching sensor IDs: {e}")
        return []

# Connect to Cassandra
@st.cache_resource
def connect_cassandra():
    try:
        cluster = Cluster(['127.0.0.1'], connection_class=LibevConnection)
        session = cluster.connect('iot_temperature')
        return session, cluster
    except Exception as e:
        st.error(f"Failed to connect to Cassandra: {e}")
        return None, None

def main():
    st.title("Temperature Monitoring Dashboard")
    
    if 'session' not in st.session_state or 'cluster' not in st.session_state:
        st.session_state.session, st.session_state.cluster = connect_cassandra()

    session = st.session_state.session
    cluster = st.session_state.cluster

    if session is None or cluster is None:
        st.error("Cannot proceed without a valid Cassandra connection.")
        return

    # Filters
    st.markdown("### Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        time_range = st.selectbox("Time Range", 
                                 ["Last Hour", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"],
                                 index=2)
    
    with col2:
        temp_filter = st.selectbox("Temperature Filter", 
                                  ["All", "Above 30°C", "Below 20°C", "Custom Range"],
                                  index=0)
        temp_min = temp_max = None
        if temp_filter == "Custom Range":
            temp_min = st.number_input("Min Temp (°C)", value=20.0, key="temp_min")
            temp_max = st.number_input("Max Temp (°C)", value=40.0, key="temp_max")
    
    with col3:
        anomaly_filter = st.selectbox("Anomaly Status", 
                                     ["All Data", "Anomalies Only", "Normal Only"],
                                     index=0)
        sensor_options = ["All"] + get_sensor_ids(session)
        sensor_ids = st.multiselect("Sensor IDs", 
                                  options=sensor_options,
                                  default=["All"])

    def update_charts():
        data = fetch_data(session, time_range, sensor_ids, temp_filter, temp_min, temp_max, anomaly_filter)

        with st.container():
            st.markdown("### Data Summary")
            if not data.empty:
                st.write(f"**Timestamp Range:** {data['timestamp'].min()} to {data['timestamp'].max()}")
                st.write(f"**Anomalies Detected:** {data['is_anomaly'].sum()}")
            else:
                st.write("No data available after filtering")

        col1, col2 = st.columns(2)

        if not data.empty:
            with col1:
                st.subheader("Temperature Over Time")
                temp_chart = alt.Chart(data).mark_line(point=True).encode(
                    x=alt.X('timestamp:T', axis=alt.Axis(format='%Y-%m-%d %H:%M:%S', title='Time', labelAngle=-45)),
                    y=alt.Y('temperature:Q', title='Temperature (°C)'),
                    color=alt.Color('is_anomaly:N', title='Anomaly', scale=alt.Scale(domain=[False, True], range=['blue', 'red'])),
                    tooltip=['timestamp:T', 'temperature:Q', 'sensor_id', 'is_anomaly:N']
                ).properties(height=400).interactive()
                st.altair_chart(temp_chart, use_container_width=True)

            with col2:
                st.subheader("Anomalies Over Time")
                anomaly_data = data[data['is_anomaly'] == True]
                if not anomaly_data.empty:
                    anomaly_chart = alt.Chart(anomaly_data).mark_point().encode(
                        x=alt.X('timestamp:T', axis=alt.Axis(format='%Y-%m-%d %H:%M:%S', title='Time', labelAngle=-45)),
                        y=alt.Y('temperature:Q', title='Temperature (°C)'),
                        color=alt.value('red'),
                        tooltip=['timestamp:T', 'temperature:Q', 'sensor_id']
                    ).properties(height=400).interactive()
                    st.altair_chart(anomaly_chart, use_container_width=True)
                else:
                    st.write("No anomalies detected")
        else:
            with col1:
                st.subheader("Temperature Over Time")
                st.write("No data available to display")
            with col2:
                st.subheader("Anomalies Over Time")
                st.write("No data available to display")

    if st.button("Apply Filters", key="apply_filters_button"):
        update_charts()

    def cleanup():
        if 'cluster' in st.session_state and st.session_state.cluster:
            st.session_state.cluster.shutdown()
        if 'session' in st.session_state and st.session_state.session:
            st.session_state.session.shutdown()

    import atexit
    atexit.register(cleanup)

if __name__ == "__main__":
    main()
