from fastapi import FastAPI , HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime , timedelta
import json
import os

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
# Cassandra configuration
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', 9042))
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'iot_data')

def get_cassandra_session():
    try:
        cluster = Cluster([CASSANDRA_HOST],port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        session.row_factory = dict_factory
        return session
    except Exception as e:
        print(f"❌ Cassandra connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Cassandra connection failed:")

def init_cassandra():
    """Create keyspace and table if not exists"""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        
        # Create keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS iot_data 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)
        
        session.set_keyspace('iot_data')
        
        # Create table
        session.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data(
                device_id TEXT,
                timestamp TIMESTAMP,
                temperature DOUBLE,
                humidity DOUBLE,
                battery_level DOUBLE,
                anomaly INT,
                PRIMARY KEY (device_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)
        
        print("Cassandra initialized successfully")
        return True
    except Exception as e:
        print(f" Cassandra initialization error: {e}")
        return False
@app.on_event("startup")
async def startup_event():
    init_cassandra()

@app.get("/api/sensors")
async def get_sensors():
    "Get All Unique Sensors"
    session = get_cassandra_session()
    query = "SELECT DISTINCT device_id FROM sensor_data"
    rows = session.execute(query)
    sensors = [row['device_id'] for row in rows]
    return sensors

@app.get("/api/sensor/{device_id}")
async def get_sensor_data(device_id:str , hour:int=24):
    session = get_cassandra_session()
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    query = """
    SELECT device_id , timestamp, temperature, humidity,battery_level, anomaly 
    FROM sensor_data
    WHERE device_id = %s AND timestamp >= %s AND timestamp <= %s
    ORDER BY timestamp DESC
    LIMIT 10
    """
    rows = session.execute(query,[device_id,start_time,end_time])
    data = {
        "device_id": device_id,
        "timestamps": [],
        "temperatures": [],
        "humidities": [],
        "battery_levels": [],
        "anomalies": [],
    }
    for row in rows:
        data["timestamps"].append(row['timestamp'].isoformat())
        data["temperatures"].append(row['temperature'])
        data["humidities"].append(row['humidity'])
        data["battery_levels"].append(row['battery_level'])
        data["anomalies"].append(row['anomaly'])

    return data


@app.get("/api/anomalies")
async def get_anomalies(hours:int = 24):
    """Get all anomalies in last N hours"""
    session = get_cassandra_session()
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    query = """
        SELECT device_id, timestamp, temperature, humidity, battery_level
        FROM sensor_data
        WHERE anomaly = 1 AND timestamp >= %s
        ORDER BY timestamp DESC
    """
    rows = session.execute(query, [start_time])
    anomalies = []
    for row in rows:
        anomalies.append({
            "device_id": row['device_id'],
            "timestamp": row['timestamp'].isoformat(),
            "temperature": row['temperature'],
            "humidity": row['humidity'],
            "battery_level": row['battery_level']
        })
    
    return {"anomalies": anomalies}

@app.get("/api/stats")
async def get_stats():
    """Get statistics dashboard"""
    session = get_cassandra_session()

    # Total readings
    total_query = "SELECT COUNT(*) as count FROM sensor_data"
    total = session.execute(total_query).one()

    #Anomaly Count
    anomaly_query =  "SELECT COUNT(*) AS count FROM sensor_data WHERE anomaly=1"
    anomaly_count =  session.execute(anomaly_query).one()

    # Last reading
    last_query = "SELECT * FROM sensor_data LIMIT 1"
    last =  session.execute(last_query).one()
    return {
        "total_readings": total['count'],
        "anomaly_count": anomaly_count['count'],
        "anomaly_percentage": (anomaly_count['count'] / total['count']) * 100 if total['count'] > 0 else 0,
        "last_reading": {
            "device_id": last['device_id'],
            "timestamp": last['timestamp'].isoformat(),
            "temperature": last['temperature'],
            "humidity":last["humidity"],
            "battery_level":last["battery_level"]
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port="8000")

