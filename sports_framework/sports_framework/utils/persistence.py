"""
Persistence Utilities
===================

Database persistence layer with connection pooling and upsert support.
"""

import pandas as pd
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
import logging

from sports_framework.core.config import settings
from sports_framework.utils.data_quality import DataQualityChecker

logger = logging.getLogger(__name__)


# Global engine for connection pooling
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Get or create database engine with connection pooling."""
    global _engine
    
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            pool_recycle=settings.db_pool_recycle,
            echo=settings.enable_debug_logging,
        )
        logger.info(f"Created database engine with pool_size={settings.db_pool_size}")
    
    return _engine


def save_dataframe(
    df: pd.DataFrame,
    table_name: str,
    unique_keys: Optional[List[str]] = None,
    if_exists: str = "append",
    chunk_size: int = 50000,
    quality_checker: Optional[DataQualityChecker] = None,
    **metadata
) -> Dict[str, Any]:
    """
    Save a DataFrame to PostgreSQL with optional upsert and data quality checks.
    
    Args:
        df: DataFrame to save
        table_name: Target table name
        unique_keys: Columns to use for upsert (e.g., ['game_id', 'sportsbook'])
        if_exists: 'append', 'replace', or 'fail'
        chunk_size: Number of rows per batch
        quality_checker: Optional DataQualityChecker instance
        **metadata: Additional metadata columns to add
    
    Returns:
        Dict with status, rows_saved, execution_time_ms, quality_report
    """
    start_time = datetime.now()
    result = {
        "status": "started",
        "rows_saved": 0,
        "execution_time_ms": 0,
        "quality_report": None,
        "error": None
    }
    
    try:
        # Data quality checks if provided
        if quality_checker:
            quality_report = quality_checker.get_summary()
            result["quality_report"] = quality_report
            
            # Fail on critical errors
            if quality_report["errors"] > 0:
                raise ValueError(f"Data quality errors: {quality_report['errors']}")
        
        # Handle empty DataFrame
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping save to {table_name}")
            result["status"] = "skipped_empty"
            return result
        
        # Add metadata columns
        df = df.copy()
        df['loaded_at'] = datetime.now()
        
        for key, value in metadata.items():
            df[f'_{key}'] = value
        
        # Normalize column names
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        
        engine = get_engine()
        
        # Use upsert if unique_keys provided
        if unique_keys and if_exists == "append":
            rows_saved = _upsert_dataframe(df, table_name, unique_keys)
        else:
            # Standard to_sql with chunking
            df.to_sql(
                table_name,
                engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            rows_saved = len(df)
        
        result.update({
            "status": "success",
            "rows_saved": rows_saved,
            "execution_time_ms": int((datetime.now() - start_time).total_seconds() * 1000)
        })
        
        logger.info(f"✓ Saved {rows_saved} rows to {table_name} in {result['execution_time_ms']}ms")
        
    except Exception as e:
        result.update({
            "status": "error",
            "error": str(e),
            "execution_time_ms": int((datetime.now() - start_time).total_seconds() * 1000)
        })
        logger.error(f"Failed to save to {table_name}: {e}")
        raise
    
    return result


def _upsert_dataframe(df: pd.DataFrame, table_name: str, unique_keys: List[str]) -> int:
    """
    Perform upsert (INSERT or UPDATE) operation using PostgreSQL's ON CONFLICT.
    
    Creates a temp table, then merges into target table.
    """
    engine = get_engine()
    temp_table = f"{table_name}_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Load new data into temp table
        df.to_sql(temp_table, engine, index=False, if_exists='replace')
        
        # Build upsert SQL
        columns = df.columns.tolist()
        set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_keys])
        
        upsert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table}
            ON CONFLICT ({', '.join(unique_keys)})
            DO UPDATE SET {set_clause}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()
            
        return result.rowcount
        
    except Exception as e:
        # Clean up temp table
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
            conn.commit()
        raise


def cleanup_old_data(
    table_name: str,
    date_column: str,
    retention_days: int = 30
) -> int:
    """
    Clean up old data from a table based on retention policy.
    
    Args:
        table_name: Table to clean
        date_column: Column containing the date/timestamp
        retention_days: Number of days to keep
    
    Returns:
        Number of rows deleted
    """
    engine = get_engine()
    
    cleanup_sql = f"""
        DELETE FROM {table_name}
        WHERE {date_column} < NOW() - INTERVAL '{retention_days} days'
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(cleanup_sql))
            conn.commit()
            
        rows_deleted = result.rowcount
        logger.info(f"Cleaned up {rows_deleted} old rows from {table_name}")
        return rows_deleted
        
    except Exception as e:
        logger.error(f"Failed to cleanup {table_name}: {e}")
        raise


def get_table_stats(table_name: str) -> Dict[str, Any]:
    """
    Get statistics about a table.
    
    Returns:
        Dict with row_count, last_updated, first_loaded, etc.
    """
    engine = get_engine()
    
    stats_sql = f"""
        SELECT 
            COUNT(*) as row_count,
            MAX(loaded_at) as last_updated,
            MIN(loaded_at) as first_loaded
        FROM {table_name}
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(stats_sql))
            row = result.fetchone()
            
        return {
            "table_name": table_name,
            "row_count": row.row_count if row and row.row_count else 0,
            "last_updated": row.last_updated if row else None,
            "first_loaded": row.first_loaded if row else None,
        }
        
    except Exception as e:
        logger.error(f"Failed to get stats for {table_name}: {e}")
        return {"error": str(e)}


def execute_query(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as DataFrame.
    
    Args:
        sql: SQL query to execute
        params: Optional parameters for the query
    
    Returns:
        Query results as DataFrame
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        return df
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    engine = get_engine()
    
    check_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
        )
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"table_name": table_name})
            return result.scalar()
    except Exception as e:
        logger.error(f"Failed to check if table exists: {e}")
        return False


# Example usage:
if __name__ == "__main__":
    import pandas as pd
    
    # Sample data
    df = pd.DataFrame({
        "game_id": ["123", "456"],
        "team_id": ["KC", "BUF"],
        "points": [28, 24]
    })
    
    # Save with upsert
    result = save_dataframe(
        df,
        table_name="test_games",
        unique_keys=["game_id"],
        if_exists="append",
        source="espn"
    )
    
    print(f"Saved {result['rows_saved']} rows in {result['execution_time_ms']}ms")
    
    # Get table stats
    stats = get_table_stats("test_games")
    print(f"Table has {stats['row_count']} rows")