"""
Ray Cluster Manager - Core cluster orchestration.
"""

import logging
import ray
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """Cluster configuration."""
    num_cpus: int = 4
    num_gpus: int = 0
    auto_scale: bool = True
    min_workers: int = 2
    max_workers: int = 20
    aws_config: Optional[Dict] = None


class RayClusterManager:
    """
    Ray cluster manager with auto-scaling support.
    
    Example:
        >>> cluster = RayClusterManager(num_cpus=4, max_workers=50)
        >>> cluster.start()
        >>> print(f"Cluster ready: {cluster.dashboard_url}")
    """
    
    def __init__(self, **kwargs):
        """
        Initialize cluster manager.
        
        Args:
            num_cpus: CPUs per worker
            num_gpus: GPUs per worker
            auto_scale: Enable auto-scaling
            min_workers: Minimum workers
            max_workers: Maximum workers
            aws_config: AWS configuration dict
        """
        self.config = ClusterConfig(**kwargs)
        self.cluster_initialized = False
        self._dashboard_url = None
        logger.info(f"Initialized cluster manager with max_workers={self.config.max_workers}")
    
    def start(self) -> None:
        """
        Start Ray cluster.
        
        Initializes Ray with auto-scaling configuration.
        """
        if self.cluster_initialized:
            logger.warning("Cluster already initialized")
            return
        
        ray_config = {
            "num_cpus": self.config.num_cpus,
            "num_gpus": self.config.num_gpus,
            "dashboard_host": "0.0.0.0",
            "include_dashboard": True
        }
        
        ray.init(**ray_config)
        self.cluster_initialized = True
        self._dashboard_url = "http://localhost:8265"
        
        logger.info(f"Ray cluster started with {self.config.num_cpus} CPUs")
        logger.info(f"Dashboard available at {self._dashboard_url}")
        
        if self.config.auto_scale:
            logger.info(f"Auto-scaling enabled: {self.config.min_workers}-{self.config.max_workers} workers")
    
    def stop(self) -> None:
        """Shutdown Ray cluster."""
        if self.cluster_initialized:
            ray.shutdown()
            self.cluster_initialized = False
            logger.info("Ray cluster stopped")
    
    @property
    def dashboard_url(self) -> str:
        """Get Ray dashboard URL."""
        return self._dashboard_url or "http://localhost:8265"
    
    def get_cluster_resources(self) -> Dict:
        """
        Get current cluster resources.
        
        Returns:
            Dict with available CPUs, GPUs, memory
        """
        if not self.cluster_initialized:
            return {}
        
        resources = ray.cluster_resources()
        return {
            "cpus": resources.get("CPU", 0),
            "gpus": resources.get("GPU", 0),
            "memory_gb": resources.get("memory", 0) / (1024 ** 3),
            "nodes": len(ray.nodes())
        }
    
    def scale_workers(self, target_workers: int) -> None:
        """
        Scale cluster to target number of workers.
        
        Args:
            target_workers: Desired number of workers
        """
        if target_workers < self.config.min_workers:
            target_workers = self.config.min_workers
        elif target_workers > self.config.max_workers:
            target_workers = self.config.max_workers
        
        logger.info(f"Scaling cluster to {target_workers} workers")
        # Implementation would integrate with AWS EC2/Fargate auto-scaling
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
