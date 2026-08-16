import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import docker
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from functools import wraps

from controller.scaler import ScalingMetrics, ScalingPolicy
from controller.utils import lifecycle
from controller.utils.models import (
    AppRegistrationResponse,
    AppSpec,
    AppStatusResponse,
    PolicyRequest,
    ScaleRequest,
    SimulatedMetricsRequest,
)

load_dotenv()

logger = logging.getLogger(__name__)


def leader_required(f):
    """Decorator to ensure only the cluster leader executes operations."""
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        cluster_controller = get_cluster_controller()
        if cluster_controller and not cluster_controller.is_leader:
            leader_info = cluster_controller.get_leader_info()
            if leader_info:
                leader_id = leader_info.get("leader_id", "unknown")
                # Return 503 so load balancer/clients can route to the active leader
                raise HTTPException(
                    status_code=503,
                    detail=f"Not the leader. Leader is: {leader_id}",
                    headers={"X-Current-Leader": str(leader_id)},
                )
            else:
                raise HTTPException(
                    status_code=503,
                    detail="No leader elected, cluster not ready",
                )
        return await f(*args, **kwargs)
    return decorated_function


# Lifespan context manager replacing deprecated @app.on_event
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycles cleanly."""
    logger.info("Initializing Orchestry Controller components...")
    await lifecycle.startup_event()
    yield
    logger.info("Shutting down Orchestry Controller components...")
    await lifecycle.shutdown_event()


# FastAPI App
app = FastAPI(
    title="Orchestry Controller API",
    description="Autoscaling controller API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Helper Getters
def get_app_manager():
    return lifecycle.get_app_manager()

def get_state_store():
    return lifecycle.get_state_store()

def get_nginx_manager():
    return lifecycle.get_nginx_manager()

def get_auto_scaler():
    return lifecycle.get_auto_scaler()

def get_health_checker():
    return lifecycle.get_health_checker()

def get_cluster_controller():
    return lifecycle.get_cluster_controller()


def _to_dict(model_or_dict):
    """Utility to safely extract dictionary from Pydantic v1/v2 models or raw dicts."""
    if hasattr(model_or_dict, "model_dump"):
        return model_or_dict.model_dump()
    elif hasattr(model_or_dict, "dict"):
        return model_or_dict.dict()
    return model_or_dict


# API Endpoints
@app.post("/apps/register", response_model=AppRegistrationResponse)
@leader_required
async def register_app(app_spec: AppSpec):
    """Register a new application."""
    try:
        spec_dict = _to_dict(app_spec)
        result = get_app_manager().register(spec_dict)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        app_name = spec_dict.get("metadata", {}).get("name")
        if not app_name:
            raise HTTPException(status_code=400, detail="App name is required in metadata")

        # Set up default scaling policy
        scaling_config = spec_dict.get("scaling", {})
        policy = ScalingPolicy(
            min_replicas=scaling_config.get("minReplicas", 1),
            max_replicas=scaling_config.get("maxReplicas", 5),
            target_rps_per_replica=scaling_config.get("targetRPSPerReplica", 50),
            max_p95_latency_ms=scaling_config.get("maxP95LatencyMs", 250),
            scale_out_threshold_pct=scaling_config.get("scaleOutThresholdPct", 80),
            scale_in_threshold_pct=scaling_config.get("scaleInThresholdPct", 30),
            window_seconds=scaling_config.get("windowSeconds", 60),
            cooldown_seconds=scaling_config.get("cooldownSeconds", 300),
        )

        get_auto_scaler().set_policy(app_name, policy)

        # Log event
        get_state_store().log_event(app_name, "registered", {"spec": spec_dict.get("spec", {})})

        return AppRegistrationResponse(
            status="registered",
            app=app_name,
            message="Application registered successfully",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to register app: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/apps/{name}/up")
@leader_required
async def start_app(name: str):
    """Start an application."""
    try:
        result = get_app_manager().start(name)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        get_state_store().log_event(name, "started", result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/apps/{name}/down")
@leader_required
async def stop_app(name: str):
    """Stop an application."""
    try:
        result = get_app_manager().stop(name)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        get_state_store().log_event(name, "stopped", result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to stop app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/apps/{name}")
@leader_required
async def delete_app(name: str):
    """Delete an application completely."""
    try:
        result = get_app_manager().delete(name)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        get_state_store().log_event(name, "deleted", result)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/apps/{name}/status", response_model=AppStatusResponse)
async def app_status(name: str):
    """Get the status of an application."""
    try:
        result = get_app_manager().status(name)

        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])

        app_record = get_state_store().get_app(name)
        app_mode = app_record.mode if app_record else "auto"

        result["mode"] = app_mode
        return AppStatusResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get status for app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/apps/{name}/scale")
@leader_required
async def scale_app(name: str, scale_request: ScaleRequest):
    """Manually scale an application."""
    try:
        result = get_app_manager().scale(name, scale_request.replicas)

        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        current_replicas = len(get_app_manager().instances.get(name, []))
        get_state_store().log_scaling_action(
            name,
            current_replicas,
            scale_request.replicas,
            "Manual scaling",
            ["manual"],
        )

        get_state_store().log_event(
            name,
            "manual_scale",
            {"old_replicas": current_replicas, "new_replicas": scale_request.replicas},
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to scale app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/apps/{name}/policy")
@leader_required
async def set_scaling_policy(name: str, policy_request: PolicyRequest):
    """Update scaling policy for an application."""
    try:
        policy_data = policy_request.policy

        policy = ScalingPolicy(
            min_replicas=policy_data.get("minReplicas", 1),
            max_replicas=policy_data.get("maxReplicas", 5),
            target_rps_per_replica=policy_data.get("targetRPSPerReplica", 50),
            max_p95_latency_ms=policy_data.get("maxP95LatencyMs", 250),
            scale_out_threshold_pct=policy_data.get("scaleOutThresholdPct", 80),
            scale_in_threshold_pct=policy_data.get("scaleInThresholdPct", 30),
            window_seconds=policy_data.get("windowSeconds", 20),
            cooldown_seconds=policy_data.get("cooldownSeconds", 30),
        )

        get_auto_scaler().set_policy(name, policy)
        get_state_store().log_event(name, "policy_updated", policy_data)

        return {"status": "updated", "app": name, "policy": policy_data}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update policy for app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/apps")
async def list_apps():
    """List all registered applications."""
    try:
        apps = get_state_store().list_apps()

        for app_item in apps:
            status_result = get_app_manager().status(app_item["name"])
            app_item["status"] = status_result.get("status", "unknown")
            app_item["replicas"] = status_result.get("replicas", 0)
            app_item["ready_replicas"] = status_result.get("ready_replicas", 0)

        return {"apps": apps}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list apps: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/apps/{name}/raw")
async def get_app_raw_spec(name: str):
    """Get the raw and parsed spec for an application."""
    try:
        parsed_spec = get_state_store().get_app(name)
        if not parsed_spec:
            raise HTTPException(status_code=404, detail=f"App {name} not found")

        raw_spec = get_state_store().get_raw_spec(name)

        return {"name": name, "raw": raw_spec, "parsed": parsed_spec}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get raw spec for app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/apps/{name}/logs")
async def get_app_logs(name: str, lines: int = 100):
    """Get logs for an application across all running container instances."""
    try:
        app_manager = get_app_manager()
        if name not in app_manager.instances:
            raise HTTPException(status_code=404, detail="App not found or not running")

        instances = app_manager.instances[name]
        if not instances:
            return {"app": name, "logs": []}

        all_logs = []
        for instance in instances:
            try:
                container = app_manager.client.containers.get(instance.container_id)
                log_output = container.logs(
                    tail=lines, timestamps=True, stdout=True, stderr=True
                )
                log_lines = log_output.decode("utf-8", errors="replace").strip().split("\n")

                for log_line in log_lines:
                    if not log_line:
                        continue

                    parts = log_line.split(" ", 1)
                    if len(parts) == 2:
                        timestamp_str, message = parts
                        try:
                            timestamp = datetime.fromisoformat(
                                timestamp_str.replace("Z", "+00:00")
                            ).timestamp()
                        except Exception:
                            timestamp = time.time()
                    else:
                        timestamp = time.time()
                        message = log_line

                    all_logs.append({
                        "timestamp": timestamp,
                        "container": instance.container_id[:12],
                        "container_full": instance.container_id,
                        "message": message,
                    })

            except docker.errors.NotFound:
                logger.warning(f"Container {instance.container_id[:12]} not found for app {name}")
                continue
            except Exception as e:
                logger.error(f"Failed to get logs from container {instance.container_id[:12]}: {e}")
                continue

        # Sort logs descending by timestamp
        all_logs.sort(key=lambda x: x["timestamp"], reverse=True)
        return {"app": name, "total_containers": len(instances), "logs": all_logs[:lines]}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get logs for app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/apps/{name}/metrics")
async def get_app_metrics(name: str):
    """Get metrics for an application."""
    try:
        metrics_summary = get_auto_scaler().get_metrics_summary(name)
        scaling_history = get_state_store().get_scaling_history(name, limit=10)

        return {
            "app": name,
            "metrics": metrics_summary,
            "scaling_history": scaling_history,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get metrics for app {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/apps/{name}/simulateMetrics")
@leader_required
async def simulate_metrics(name: str, sim: SimulatedMetricsRequest):
    """Inject simulated metrics for an app and optionally trigger autoscaling evaluation."""
    try:
        app_mgr = get_app_manager()
        if name not in app_mgr.instances:
            raise HTTPException(status_code=404, detail="App not running")

        instances = app_mgr.instances[name]
        replica_count = len(instances)
        healthy = sum(1 for i in instances if i.state == "ready")
        healthy_replicas = sim.healthyReplicas if sim.healthyReplicas is not None else healthy

        metrics = ScalingMetrics(
            rps=sim.rps,
            p95_latency_ms=sim.p95LatencyMs,
            active_connections=sim.activeConnections,
            cpu_percent=sim.cpuPercent,
            memory_percent=sim.memoryPercent,
            healthy_replicas=healthy_replicas,
            total_replicas=replica_count,
        )
        get_auto_scaler().add_metrics(name, metrics)

        evaluation = None
        action = None
        if sim.evaluate:
            app_record = get_state_store().get_app(name)
            app_mode = app_record.mode if app_record else "auto"

            evaluation = get_auto_scaler().evaluate_scaling(name, replica_count, mode=app_mode)
            if evaluation.should_scale:
                result = app_mgr.scale(name, evaluation.target_replicas)
                if result.get("status") == "scaled":
                    get_auto_scaler().record_scaling_action(name, evaluation.target_replicas)
                    get_state_store().log_scaling_action(
                        name,
                        evaluation.current_replicas,
                        evaluation.target_replicas,
                        evaluation.reason,
                        evaluation.triggered_by,
                        evaluation.metrics.__dict__ if evaluation.metrics else None,
                    )
                    action = {
                        "scaled": True,
                        "from": evaluation.current_replicas,
                        "to": evaluation.target_replicas,
                        "reason": evaluation.reason,
                    }
                else:
                    action = {"scaled": False, "error": result}

        return {
            "app": name,
            "metrics_added": metrics.__dict__,
            "evaluation": {
                "should_scale": evaluation.should_scale if evaluation else None,
                "target_replicas": evaluation.target_replicas if evaluation else None,
                "reason": evaluation.reason if evaluation else None,
                "scale_factors": get_auto_scaler().last_scale_factors.get(name),
            } if evaluation else None,
            "action": action,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to simulate metrics for {name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def get_system_metrics():
    """Get system-wide metrics for monitoring."""
    try:
        all_apps = get_state_store().list_apps()
        total_apps = len(all_apps)
        running_apps = 0
        total_instances = 0
        healthy_instances = 0

        app_mgr = get_app_manager()
        for app in all_apps:
            app_name = app["name"]
            if app_name in app_mgr.instances:
                instances = app_mgr.instances[app_name]
                if instances:
                    running_apps += 1
                    total_instances += len(instances)
                    healthy_instances += sum(1 for inst in instances if inst.state == "ready")

        nginx_status = get_nginx_manager().get_nginx_status()
        health_summary = get_health_checker().get_health_summary()
        cluster_ctrl = get_cluster_controller()

        return {
            "timestamp": time.time(),
            "cluster": cluster_ctrl.get_cluster_status() if cluster_ctrl else None,
            "apps": {
                "total": total_apps,
                "running": running_apps,
            },
            "instances": {
                "total": total_instances,
                "healthy": healthy_instances,
                "unhealthy": total_instances - healthy_instances,
            },
            "nginx": nginx_status,
            "health_checks": health_summary,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get system metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events")
async def get_events(app: Optional[str] = None, limit: int = 100):
    """Get recent events."""
    try:
        events = get_state_store().get_events(app, limit)
        return {"events": events}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cluster/status")
async def get_cluster_status():
    """Get detailed cluster status and membership."""
    cluster_ctrl = get_cluster_controller()
    if not cluster_ctrl:
        raise HTTPException(status_code=503, detail="Clustering not enabled")

    try:
        return cluster_ctrl.get_cluster_status()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get cluster status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/cluster/health")
async def cluster_health():
    return {"status": "healthy", "cluster": "ok"}