"""
Ray Aerospace Compute - Distributed compute for aerospace simulations.
"""

from .cluster.manager import RayClusterManager, ClusterConfig
from .jobs.submitter import JobSubmitter
from .jobs.models import Job, JobStatus

__version__ = "0.1.0"

__all__ = [
    "RayClusterManager",
    "ClusterConfig",
    "JobSubmitter",
    "Job",
    "JobStatus",
]
