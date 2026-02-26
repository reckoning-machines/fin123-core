"""Re-export of the artifact store from versioning module.

The canonical implementation lives in ``fin123.versioning.ArtifactStore``.
This module provides a convenient import path.
"""

from fin123.versioning import ArtifactStore

__all__ = ["ArtifactStore"]
