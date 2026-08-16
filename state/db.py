import psycopg2
import psycopg2.pool
import json
import time
import logging
import threading
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom database error for better error handling."""
    pass

@dataclass
class AppRecord:
    """Application record stored in the database."""
    name: str
    spec: Dict[str, Any]
    status: str  # registered, running, stopped, error
    created_at: float
    updated_at: float
    replicas: int = 0
    last_scaled_at: Optional[float] = None
    mode: str = 'auto'  # 'auto' or 'manual'

@dataclass
class InstanceRecord:
    """Container instance record."""
    app_name: str
    container_id: str
    ip: str
    port: int
    status: str  # starting, ready, unhealthy, stopping, stopped
    created_at: float
    updated_at: float
    failure_count: int = 0
    last_health_check: Optional[float] = None

@dataclass
class EventRecord:
    """System event record for audit trail."""
    id: Optional[int]
    app_name: str
    event_type: str  # scaling, health, config, error
    message: str
    timestamp: float
    details: Optional[Dict[str, Any]] = None

class PostgreSQLManager:
    """
    PostgreSQL-based high availability persistent storage for Orchestry.
    Thread-safe with connection pooling, read/write splitting, and automatic failover.
    
    Features:
    - Primary/Replica architecture for high availability
    - Connection pooling for performance
    - Automatic failover from replica to primary
    - Read/write operation splitting
    - Thread-safe operations
    """
    
    def __init__(self, 
                 primary_host: str = "postgres-primary", 
                 primary_port: int = 5432,
                 replica_host: str = "postgres-replica", 
                 replica_port: int = 5432,
                 database: str = "orchestry",
                 username: str = "orchestry",
                 password: str = "CONTAINER_ORCH_password",
                 min_conn: int = 5,
                 max_conn: int = 20):
        
        self.primary_dsn = f"host={primary_host} port={primary_port} dbname={database} user={username} password={password}"
        self.replica_dsn = f"host={replica_host} port={replica_port} dbname={database} user={username} password={password}"
        self._lock = threading.RLock()
        
        # Connection pools
        self._primary_pool = None
        self._replica_pool = None
        self._min_conn = min_conn
        self._max_conn = max_conn
        
        # Failover state tracking
        self._primary_failed = False
        self._last_primary_check = 0
        self._primary_check_interval = 30  # Check primary every 30 seconds
        
        # Initialize connection pools and database
        self._init_connection_pools()
        self._init_database()
        
    def _init_connection_pools(self):
        """Initialize connection pools for primary and replica."""
        logger.info(f"🔗 Connecting to Primary: {self.primary_dsn}")
        logger.info(f"🔗 Connecting to Replica: {self.replica_dsn}")
        
        try:
            # Test primary connection first
            test_conn = psycopg2.connect(self.primary_dsn)
            with test_conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                logger.info(f"✅ Primary database ready: {version[:50]}...")
            test_conn.close()
            
            # Primary connection pool (required)
            self._primary_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._min_conn,
                maxconn=self._max_conn,
                dsn=self.primary_dsn
            )
            logger.info("✅ Primary PostgreSQL connection pool initialized")
            
            # Replica connection pool (optional, for read operations)
            try:
                # Test replica connection
                test_conn = psycopg2.connect(self.replica_dsn)
                with test_conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    logger.info(f"✅ Replica database ready: {version[:50]}...")
                test_conn.close()
                
                self._replica_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self._min_conn,
                    maxconn=self._max_conn,
                    dsn=self.replica_dsn
                )
                logger.info("✅ Replica PostgreSQL connection pool initialized")
            except Exception as e:
                logger.warning(f"⚠️ Replica connection pool failed, will use primary for reads: {e}")
                self._replica_pool = None
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize PostgreSQL connection pools: {e}")
            raise RuntimeError(f"Cannot initialize PostgreSQL HA cluster: {e}") from e
        
    def _init_database(self):
        """Initialize database schema with proper indexes for performance."""
        with self._get_connection(write=True) as conn:
            with conn.cursor() as cursor:
                # Apps table - stores application configurations
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS apps (
                        name VARCHAR(255) PRIMARY KEY,
                        spec JSONB NOT NULL,
                        status VARCHAR(50) NOT NULL DEFAULT 'registered',
                        created_at DOUBLE PRECISION NOT NULL,
                        updated_at DOUBLE PRECISION NOT NULL,
                        replicas INTEGER DEFAULT 0,
                        last_scaled_at DOUBLE PRECISION,
                        mode VARCHAR(10) DEFAULT 'auto'
                    )
                ''')
                
                # Instances table - stores container instance information
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS instances (
                        container_id VARCHAR(255) PRIMARY KEY,
                        app_name VARCHAR(255) NOT NULL,
                        ip VARCHAR(45) NOT NULL,
                        port INTEGER NOT NULL,
                        status VARCHAR(50) NOT NULL DEFAULT 'starting',
                        created_at DOUBLE PRECISION NOT NULL,
                        updated_at DOUBLE PRECISION NOT NULL,
                        failure_count INTEGER DEFAULT 0,
                        last_health_check DOUBLE PRECISION,
                        FOREIGN KEY (app_name) REFERENCES apps (name) ON DELETE CASCADE
                    )
                ''')
                
                # Events table - stores system events and audit trail
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        app_name VARCHAR(255) NOT NULL,
                        event_type VARCHAR(100) NOT NULL,
                        message TEXT NOT NULL,
                        timestamp DOUBLE PRECISION NOT NULL,
                        details JSONB
                    )
                ''')
                
                # Scaling history table - tracks scaling operations
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS scaling_history (
                        id SERIAL PRIMARY KEY,
                        app_name VARCHAR(255) NOT NULL,
                        from_replicas INTEGER NOT NULL,
                        to_replicas INTEGER NOT NULL,
                        trigger_reason TEXT NOT NULL,
                        metrics_snapshot JSONB,
                        timestamp DOUBLE PRECISION NOT NULL
                    )
                ''')
                
                # Performance indexes
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_app_time ON events (app_name, timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_apps_status ON apps (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_apps_mode ON apps (mode)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_instances_app ON instances (app_name)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_instances_status ON instances (status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_scaling_app_time ON scaling_history (app_name, timestamp)')
                
                conn.commit()
                
        logger.info("🎉 PostgreSQL database schema initialized successfully")
    
    def _mark_primary_failed(self):
        """Mark primary as failed and record the failure time."""
        self._primary_failed = True
        self._last_primary_check = time.time()
        logger.error("🚨 PRIMARY DATABASE MARKED AS FAILED")
    
    def _check_primary_recovery(self):
        """Check if primary database has recovered."""
        if not self._primary_pool:
            return
            
        try:
            conn = self._primary_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            self._primary_pool.putconn(conn)
            
            # Primary is back!
            self._primary_failed = False
            logger.info("✅ PRIMARY DATABASE RECOVERED")
            
        except Exception as e:
            logger.debug(f"Primary still failed: {e}")
            self._last_primary_check = time.time()
        
    @contextmanager
    def _get_connection(self, write: bool = False):
        """
        Get a database connection with intelligent routing and failover.
        - Write operations prefer primary, fallback to replica if primary fails
        - Read operations prefer replica, fallback to primary if replica fails
        """
        # Check if we should retry primary connection
        current_time = time.time()
        if self._primary_failed and (current_time - self._last_primary_check) > self._primary_check_interval:
            self._check_primary_recovery()
        
        conn = None
        pool_used = None
        connection_acquired = False
        
        try:
            if write:
                # For writes, try primary first
                if not self._primary_failed and self._primary_pool:
                    try:
                        pool_used = self._primary_pool
                        conn = pool_used.getconn()
                        conn.autocommit = False
                        connection_acquired = True
                        yield conn
                        return
                    except Exception as e:
                        logger.error(f"Primary database failed for write: {e}")
                        self._mark_primary_failed()
                        if conn and connection_acquired:
                            try:
                                conn.rollback()
                            except:
                                pass
                            try:
                                pool_used.putconn(conn)
                            except:
                                pass
                        conn = None
                        pool_used = None
                        connection_acquired = False
                        raise
                
                # Primary failed, try replica for writes (emergency mode)
                if self._replica_pool:
                    try:
                        pool_used = self._replica_pool
                        conn = pool_used.getconn()
                        logger.warning("🚨 USING REPLICA FOR WRITE OPERATION (PRIMARY DOWN)")
                        conn.autocommit = False
                        connection_acquired = True
                        yield conn
                        return
                    except Exception as e:
                        logger.error(f"Replica also failed for write: {e}")
                        if conn and connection_acquired:
                            try:
                                conn.rollback()
                            except:
                                pass
                            try:
                                pool_used.putconn(conn)
                            except:
                                pass
                        raise
                
                raise DatabaseError("❌ NO DATABASE AVAILABLE FOR WRITE OPERATIONS")
            
            else:
                # For reads, try replica first
                if self._replica_pool:
                    try:
                        pool_used = self._replica_pool
                        conn = pool_used.getconn()
                        conn.autocommit = False
                        connection_acquired = True
                        yield conn
                        return
                    except Exception as e:
                        logger.warning(f"Replica failed for read, trying primary: {e}")
                        if conn and connection_acquired:
                            try:
                                conn.rollback()
                            except:
                                pass
                            try:
                                pool_used.putconn(conn)
                            except:
                                pass
                        conn = None
                        pool_used = None
                        connection_acquired = False
                        # Don't raise here, try primary next
                
                # Replica failed, try primary for reads
                if not self._primary_failed and self._primary_pool:
                    try:
                        pool_used = self._primary_pool
                        conn = pool_used.getconn()
                        conn.autocommit = False
                        connection_acquired = True
                        yield conn
                        return
                    except Exception as e:
                        logger.error(f"Primary also failed for read: {e}")
                        self._mark_primary_failed()
                        if conn and connection_acquired:
                            try:
                                conn.rollback()
                            except:
                                pass
                            try:
                                pool_used.putconn(conn)
                            except:
                                pass
                        raise
                
                raise DatabaseError("❌ NO DATABASE AVAILABLE FOR READ OPERATIONS")
                
        except DatabaseError:
            # Re-raise database errors as-is
            raise
        except Exception as e:
            # Wrap other exceptions
            if conn and connection_acquired:
                try:
                    conn.rollback()
                except:
                    pass
            raise DatabaseError(f"Database operation failed: {e}")
        finally:
            if conn and pool_used and connection_acquired:
                try:
                    pool_used.putconn(conn)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
            
    # App management
    def save_app(self, app_record: AppRecord) -> bool:
        """Save or update an application record."""
        with self._lock:
            try:
                with self._get_connection(write=True) as conn:
                    with conn.cursor() as cursor:
                        # Ensure spec is properly serialized as JSON
                        spec_json = app_record.spec
                        if isinstance(spec_json, dict):
                            spec_json = json.dumps(spec_json)
                        elif not isinstance(spec_json, str):
                            logger.error(f"Invalid spec type for app {app_record.name}: {type(spec_json)}")
                            spec_json = json.dumps({})
                        
                        cursor.execute('''
                            INSERT INTO apps 
                            (name, spec, status, created_at, updated_at, replicas, last_scaled_at, mode)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (name) DO UPDATE SET
                                spec = EXCLUDED.spec,
                                status = EXCLUDED.status,
                                updated_at = EXCLUDED.updated_at,
                                replicas = EXCLUDED.replicas,
                                last_scaled_at = EXCLUDED.last_scaled_at,
                                mode = EXCLUDED.mode
                        ''', (
                            app_record.name,
                            spec_json,
                            app_record.status,
                            app_record.created_at,
                            app_record.updated_at,
                            app_record.replicas,
                            app_record.last_scaled_at,
                            app_record.mode
                        ))
                        conn.commit()
                        return True
            except Exception as e:
                logger.error(f"Failed to save app {app_record.name}: {e}")
                return False
                
    def get_app(self, name: str) -> Optional[AppRecord]:
        """Get an application record by name."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute('SELECT * FROM apps WHERE name = %s', (name,))
                        row = cursor.fetchone()
                        if row:
                            # Handle spec field - could be JSON string or dict
                            spec_data = row[1]
                            if isinstance(spec_data, str):
                                spec = json.loads(spec_data)
                            elif isinstance(spec_data, dict):
                                spec = spec_data
                            else:
                                logger.warning(f"Unexpected spec type for app {name}: {type(spec_data)}")
                                spec = {}
                                
                            return AppRecord(
                                name=row[0],
                                spec=spec,
                                status=row[2],
                                created_at=row[3],
                                updated_at=row[4],
                                replicas=row[5],
                                last_scaled_at=row[6],
                                mode=row[7] if row[7] else 'auto'
                            )
            except Exception as e:
                logger.error(f"Failed to get app {name}: {e}")
        return None
        def list_apps(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
         """List all applications, optionally filtered by status."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor() as cursor:
                        if status:
                            cursor.execute('SELECT * FROM apps WHERE status = %s ORDER BY name', (status,))
                        else:
                            cursor.execute('SELECT * FROM apps ORDER BY name')
                        
                        rows = cursor.fetchall()
                        apps = []
                        for row in rows:
                            spec_data = row[1]
                            if isinstance(spec_data, str):
                                spec = json.loads(spec_data)
                            elif isinstance(spec_data, dict):
                                spec = spec_data
                            else:
                                spec = {}
                                
                            apps.append({
                                'name': row[0],
                                'spec': spec,
                                'status': row[2],
                                'created_at': row[3],
                                'updated_at': row[4],
                                'replicas': row[5],
                                'last_scaled_at': row[6],
                                'mode': row[7] if row[7] else 'auto'
                            })
                        return apps
            except Exception as e:
                logger.error(f"Failed to list apps: {e}")
                return []

    def delete_app(self, name: str) -> bool:
        """Delete an application record."""
        with self._lock:
            try:
                with self._get_connection(write=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute('DELETE FROM apps WHERE name = %s', (name,))
                        conn.commit()
                        return True
            except Exception as e:
                logger.error(f"Failed to delete app {name}: {e}")
                return False

    def log_event(self, app_name: str, event_type: str, details: Optional[Dict[str, Any]] = None, message: str = ""):
        """Log a system event."""
        try:
            details_json = json.dumps(details) if isinstance(details, dict) else details
            with self._get_connection(write=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO events (app_name, event_type, message, timestamp, details)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (app_name, event_type, message or f"Event {event_type} triggered", time.time(), details_json))
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to log event for {app_name}: {e}")

    def get_events(self, app_name: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve recent events."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor() as cursor:
                        if app_name:
                            cursor.execute(
                                'SELECT id, app_name, event_type, message, timestamp, details FROM events WHERE app_name = %s ORDER BY timestamp DESC LIMIT %s',
                                (app_name, limit)
                            )
                        else:
                            cursor.execute('SELECT id, app_name, event_type, message, timestamp, details FROM events ORDER BY timestamp DESC LIMIT %s', (limit,))
                        
                        rows = cursor.fetchall()
                        events = []
                        for r in rows:
                            det = r[5]
                            if isinstance(det, str):
                                det = json.loads(det)
                            events.append({
                                'id': r[0],
                                'app_name': r[1],
                                'event_type': r[2],
                                'message': r[3],
                                'timestamp': r[4],
                                'details': det
                            })
                        return events
            except Exception as e:
                logger.error(f"Failed to fetch events: {e}")
                return []

    def log_scaling_action(self, app_name: str, from_replicas: int, to_replicas: int, reason: str, triggered_by: Optional[List[str]] = None, metrics_snapshot: Optional[Dict[str, Any]] = None):
        """Log an autoscaling action."""
        try:
            snapshot = metrics_snapshot or {}
            if triggered_by:
                snapshot['triggered_by'] = triggered_by

            with self._get_connection(write=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO scaling_history 
                        (app_name, from_replicas, to_replicas, trigger_reason, metrics_snapshot, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (app_name, from_replicas, to_replicas, reason, json.dumps(snapshot), time.time()))
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to log scaling action for {app_name}: {e}")

    def get_scaling_history(self, app_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch scaling history records for an app."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            'SELECT id, app_name, from_replicas, to_replicas, trigger_reason, metrics_snapshot, timestamp FROM scaling_history WHERE app_name = %s ORDER BY timestamp DESC LIMIT %s',
                            (app_name, limit)
                        )
                        rows = cursor.fetchall()
                        history = []
                        for r in rows:
                            snap = r[5]
                            if isinstance(snap, str):
                                snap = json.loads(snap)
                            history.append({
                                'id': r[0],
                                'app_name': r[1],
                                'from_replicas': r[2],
                                'to_replicas': r[3],
                                'reason': r[4],
                                'metrics_snapshot': snap,
                                'timestamp': r[6]
                            })
                        return history
            except Exception as e:
                logger.error(f"Failed to fetch scaling history for {app_name}: {e}")
                return []

    def close(self):
        """Shutdown database pools gracefully."""
        with self._lock:
            if self._primary_pool:
                self._primary_pool.closeall()
                logger.info("Primary database connection pool closed")
            if self._replica_pool:
                self._replica_pool.closeall()
                logger.info("Replica database connection pool closed")