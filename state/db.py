import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom database error for Orchestry state management."""
    pass


@dataclass
class AppRecord:
    """Application record stored in PostgreSQL."""
    name: str
    spec: Dict[str, Any]
    status: str  # registered, running, stopped, error
    created_at: float
    updated_at: float
    replicas: int = 0
    last_scaled_at: Optional[float] = None
    mode: str = "auto"  # 'auto' or 'manual'


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
    """System audit trail event record."""
    id: Optional[int]
    app_name: str
    event_type: str  # scaling, health, config, error
    message: str
    timestamp: float
    details: Optional[Dict[str, Any]] = None


class PostgreSQLManager:
    """
    PostgreSQL-based high-availability persistent storage for Orchestry.
    Thread-safe connection pooling with read/write splitting and automatic failover.
    """

    def __init__(
        self,
        primary_host: str = os.getenv("POSTGRES_PRIMARY_HOST", "postgres-primary"),
        primary_port: int = int(os.getenv("POSTGRES_PRIMARY_PORT", 5432)),
        replica_host: str = os.getenv("POSTGRES_REPLICA_HOST", "postgres-replica"),
        replica_port: int = int(os.getenv("POSTGRES_REPLICA_PORT", 5432)),
        database: str = os.getenv("POSTGRES_DB", "orchestry"),
        username: str = os.getenv("POSTGRES_USER", "orchestry"),
        password: str = os.getenv("POSTGRES_PASSWORD", "CONTAINER_ORCH_password"),
        min_conn: int = 2,
        max_conn: int = 20,
    ):
        self.primary_dsn = f"host={primary_host} port={primary_port} dbname={database} user={username} password={password}"
        self.replica_dsn = f"host={replica_host} port={replica_port} dbname={database} user={username} password={password}"
        self._lock = threading.RLock()

        self._primary_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._replica_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._min_conn = min_conn
        self._max_conn = max_conn

        self._primary_failed = False
        self._last_primary_check = 0.0
        self._primary_check_interval = 30.0

        self._init_connection_pools()
        self._init_database()

    def _init_connection_pools(self):
        """Initialize connection pools for primary and replica nodes."""
        logger.info("Connecting to Primary PostgreSQL pool...")
        try:
            test_conn = psycopg2.connect(self.primary_dsn, connect_timeout=5)
            with test_conn.cursor() as cur:
                cur.execute("SELECT version()")
                logger.info("Primary DB verified successfully.")
            test_conn.close()

            self._primary_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._min_conn,
                maxconn=self._max_conn,
                dsn=self.primary_dsn,
            )
        except Exception as e:
            logger.error(f"Failed to connect to Primary DB pool: {e}")
            raise RuntimeError(f"Cannot initialize Primary DB: {e}") from e

        # Optional Replica pool setup for reads
        try:
            test_conn = psycopg2.connect(self.replica_dsn, connect_timeout=5)
            with test_conn.cursor() as cur:
                cur.execute("SELECT version()")
                logger.info("Replica DB verified successfully.")
            test_conn.close()

            self._replica_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._min_conn,
                maxconn=self._max_conn,
                dsn=self.replica_dsn,
            )
        except Exception as e:
            logger.warning(f"Replica DB offline. Routing all queries to Primary: {e}")
            self._replica_pool = None

    def _init_database(self):
        """Initialize database schema and indexes."""
        with self._get_connection(write=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS apps (
                        name VARCHAR(255) PRIMARY KEY,
                        spec JSONB NOT NULL,
                        raw_spec JSONB,
                        status VARCHAR(50) NOT NULL DEFAULT 'registered',
                        created_at DOUBLE PRECISION NOT NULL,
                        updated_at DOUBLE PRECISION NOT NULL,
                        replicas INTEGER DEFAULT 0,
                        last_scaled_at DOUBLE PRECISION,
                        mode VARCHAR(10) DEFAULT 'auto'
                    );
                """)

                cursor.execute("""
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
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        app_name VARCHAR(255) NOT NULL,
                        event_type VARCHAR(100) NOT NULL,
                        message TEXT NOT NULL,
                        timestamp DOUBLE PRECISION NOT NULL,
                        details JSONB
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS scaling_history (
                        id SERIAL PRIMARY KEY,
                        app_name VARCHAR(255) NOT NULL,
                        from_replicas INTEGER NOT NULL,
                        to_replicas INTEGER NOT NULL,
                        trigger_reason TEXT NOT NULL,
                        metrics_snapshot JSONB,
                        timestamp DOUBLE PRECISION NOT NULL
                    );
                """)

                # Performance indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_app_time ON events (app_name, timestamp);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_apps_status ON apps (status);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_app ON instances (app_name);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_scaling_app_time ON scaling_history (app_name, timestamp);")
                conn.commit()

        logger.info("PostgreSQL schema initialized successfully.")

    def _check_primary_recovery(self):
        """Check if primary DB node has recovered from failover state."""
        if not self._primary_pool:
            return
        try:
            conn = self._primary_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            self._primary_pool.putconn(conn)
            self._primary_failed = False
            logger.info("Primary DB node HAS RECOVERED.")
        except Exception as e:
            logger.debug(f"Primary DB still unresponsive: {e}")
            self._last_primary_check = time.time()

    @contextmanager
    def _get_connection(self, write: bool = False):
        """Smart thread context manager for read/write pool routing."""
        current_time = time.time()
        if self._primary_failed and (current_time - self._last_primary_check) > self._primary_check_interval:
            self._check_primary_recovery()

        pool_to_use = self._primary_pool if write else (self._replica_pool or self._primary_pool)

        if not pool_to_use:
            raise DatabaseError("No active PostgreSQL pool available.")

        conn = None
        try:
            conn = pool_to_use.getconn()
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if write and pool_to_use == self._primary_pool:
                self._primary_failed = True
                self._last_primary_check = time.time()
            raise DatabaseError(f"Database operation failed: {e}") from e
        finally:
            if conn and pool_to_use:
                try:
                    pool_to_use.putconn(conn)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")

    # App Operations
    def save_app(self, app_record: AppRecord, raw_spec: Optional[Dict[str, Any]] = None) -> bool:
        """Save or update application record."""
        with self._lock:
            try:
                spec_json = json.dumps(app_record.spec) if isinstance(app_record.spec, dict) else app_record.spec
                raw_spec_json = json.dumps(raw_spec) if isinstance(raw_spec, dict) else raw_spec

                with self._get_connection(write=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO apps 
                            (name, spec, raw_spec, status, created_at, updated_at, replicas, last_scaled_at, mode)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (name) DO UPDATE SET
                                spec = EXCLUDED.spec,
                                raw_spec = COALESCE(EXCLUDED.raw_spec, apps.raw_spec),
                                status = EXCLUDED.status,
                                updated_at = EXCLUDED.updated_at,
                                replicas = EXCLUDED.replicas,
                                last_scaled_at = EXCLUDED.last_scaled_at,
                                mode = EXCLUDED.mode
                        """, (
                            app_record.name,
                            spec_json,
                            raw_spec_json,
                            app_record.status,
                            app_record.created_at,
                            app_record.updated_at,
                            app_record.replicas,
                            app_record.last_scaled_at,
                            app_record.mode,
                        ))
                        conn.commit()
                return True
            except Exception as e:
                logger.error(f"Failed to save app {app_record.name}: {e}")
                return False

    def get_app(self, name: str) -> Optional[AppRecord]:
        """Fetch single app by name."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute("SELECT * FROM apps WHERE name = %s", (name,))
                        row = cursor.fetchone()
                        if row:
                            spec_data = row["spec"]
                            if isinstance(spec_data, str):
                                spec_data = json.loads(spec_data)

                            return AppRecord(
                                name=row["name"],
                                spec=spec_data,
                                status=row["status"],
                                created_at=row["created_at"],
                                updated_at=row["updated_at"],
                                replicas=row["replicas"],
                                last_scaled_at=row["last_scaled_at"],
                                mode=row["mode"] or "auto",
                            )
            except Exception as e:
                logger.error(f"Failed to fetch app {name}: {e}")
        return None

    def get_raw_spec(self, name: str) -> Optional[Dict[str, Any]]:
        """Fetch raw unparsed spec submitted by user."""
        with self._lock:
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT raw_spec FROM apps WHERE name = %s", (name,))
                        row = cursor.fetchone()
                        if row and row[0]:
                            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
            except Exception as e:
                logger.error(f"Failed to fetch raw spec for app {name}: {e}")
        return None

    def list_apps(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all applications with optional status filtering."""
        with self._lock:
            apps = []
            try:
                with self._get_connection(write=False) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        if status:
                            cursor.execute("SELECT * FROM apps WHERE status = %s ORDER BY name", (status,))
                        else:
                            cursor.execute("SELECT * FROM apps ORDER BY name")
                        
                        rows = cursor.fetchall()
                        for row in rows:
                            spec_data = row["spec"]
                            if isinstance(spec_data, str):
                                spec_data = json.loads(spec_data)

                            apps.append({
                                "name": row["name"],
                                "spec": spec_data,
                                "status": row["status"],
                                "created_at": row["created_at"],
                                "updated_at": row["updated_at"],
                                "replicas": row["replicas"],
                                "last_scaled_at": row["last_scaled_at"],
                                "mode": row["mode"] or "auto",
                            })
            except Exception as e:
                logger.error(f"Failed to list apps: {e}")
            return apps

    def delete_app(self, name: str) -> bool:
        """Delete application record and cascade containers."""
        with self._lock:
            try:
                with self._get_connection(write=True) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("DELETE FROM apps WHERE name = %s", (name,))
                        conn.commit()
                return True
            except Exception as e:
                logger.error(f"Failed to delete app {name}: {e}")
                return False

    # Event Logging
    def log_event(self, app_name: str, event_type: str, details: Optional[Dict[str, Any]] = None, message: str = ""):
        """Log audit trail event into database."""
        try:
            details_json = json.dumps(details) if isinstance(details, dict) else details
            with self._get_connection(write=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO events (app_name, event_type, message, timestamp, details)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (app_name, event_type, message or f"Event {event_type} triggered", time.time(), details_json))
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to log event for {app_name}: {e}")

    def get_events(self, app_name: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch recent event logs."""
        try:
            with self._get_connection(write=False) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    if app_name:
                        cursor.execute(
                            "SELECT * FROM events WHERE app_name = %s ORDER BY timestamp DESC LIMIT %s",
                            (app_name, limit),
                        )
                    else:
                        cursor.execute("SELECT * FROM events ORDER BY timestamp DESC LIMIT %s", (limit,))
                    
                    rows = cursor.fetchall()
                    events = []
                    for r in rows:
                        det = r["details"]
                        if isinstance(det, str):
                            det = json.loads(det)
                        events.append({
                            "id": r["id"],
                            "app_name": r["app_name"],
                            "event_type": r["event_type"],
                            "message": r["message"],
                            "timestamp": r["timestamp"],
                            "details": det,
                        })
                    return events
        except Exception as e:
            logger.error(f"Failed to fetch events: {e}")
            return []

    # Scaling History
    def log_scaling_action(
        self,
        app_name: str,
        from_replicas: int,
        to_replicas: int,
        reason: str,
        triggered_by: Optional[List[str]] = None,
        metrics_snapshot: Optional[Dict[str, Any]] = None,
    ):
        """Log autoscaler actions."""
        try:
            snapshot = metrics_snapshot or {}
            if triggered_by:
                snapshot["triggered_by"] = triggered_by

            with self._get_connection(write=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO scaling_history 
                        (app_name, from_replicas, to_replicas, trigger_reason, metrics_snapshot, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (app_name, from_replicas, to_replicas, reason, json.dumps(snapshot), time.time()))
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to log scaling action for {app_name}: {e}")

    def get_scaling_history(self, app_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get scaling history for app."""
        try:
            with self._get_connection(write=False) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT * FROM scaling_history WHERE app_name = %s ORDER BY timestamp DESC LIMIT %s",
                        (app_name, limit),
                    )
                    rows = cursor.fetchall()
                    history = []
                    for r in rows:
                        snap = r["metrics_snapshot"]
                        if isinstance(snap, str):
                            snap = json.loads(snap)
                        history.append({
                            "id": r["id"],
                            "app_name": r["app_name"],
                            "from_replicas": r["from_replicas"],
                            "to_replicas": r["to_replicas"],
                            "reason": r["trigger_reason"],
                            "metrics_snapshot": snap,