#create two endpoints for the FastAPI application
from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict, Any
import duckdb
import os
from datetime import datetime
from pathlib import Path

app = FastAPI(
    title="DuckLake Query API",
    description="API for querying DuckLake data with GEO and NAICS filtering",
    version="1.0.0"
)

DUCKLAKE_BASE = os.getenv("DUCKLAKE_BASE", str(Path.home() / "Documents" / "sample_ducklake"))
LAKE_DB_PATH = f"{DUCKLAKE_BASE}/lake.duckdb"

def get_ducklake_connection():
    """Get a connection to the ducklake database"""
    try:
        conn = duckdb.connect(database=LAKE_DB_PATH)
        
        # Load ducklake extension
        conn.execute("INSTALL 'ducklake'")
        conn.execute("LOAD 'ducklake'")
        
        # Attach the ducklake catalog
        catalog_path = f"{DUCKLAKE_BASE}/catalog.duckdb"
        data_path = f"{DUCKLAKE_BASE}/data"
        
        conn.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_lake (DATA_PATH '{data_path}')")
        conn.execute("USE my_lake")
        
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

## endpoint 1 queries your ducklake
@app.get("/", tags=["Health"])
def health_check():
    current_time = datetime.now().isoformat()
    return {
        "status": "healthy",
        "message": "DuckLake Query API is running",
        "timestamp": current_time,
        "service": "ducklake-api"
    }
## endpoint 2 is root and it is a health check that returns the current time stamp
@app.get("/query_ducklake", tags=["Query"])
def query_ducklake(
    limit: int = Query(100, description="Maximum number of records to return", ge=1, le=10000)
) -> Dict[str, Any]:
    conn = None
    try:
        conn = get_ducklake_connection()
        query = f"SELECT * FROM canada_testkit LIMIT {limit}"
        result = conn.execute(query).fetchall()

        return {
            "status": "success",
            "count": len(result),
            "limit": limit,
            "data": result,
            "query_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
    
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
 