"""Utility for calculating dynamic timeouts based on historical chunk completion times."""
import os
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Optional


class TimeoutCalculator:
    """Calculate dynamic timeouts for occupancy chunk jobs."""

    def __init__(self):
        self.multiplier = float(os.getenv("CHUNK_TIMEOUT_MULTIPLIER", "2.0"))
        self.min_timeout = int(os.getenv("CHUNK_TIMEOUT_MIN_SECONDS", "900"))
        self.lookback_days = int(os.getenv("CHUNK_TIMEOUT_LOOKBACK_DAYS", "30"))

    def calculate_timeout(
        self,
        session: Session,
        interval_seconds: int,
        space_type: str
    ) -> int:
        """
        Calculate dynamic timeout based on historical average completion time.

        Args:
            session: Database session
            interval_seconds: Time bin granularity (e.g., 3600 for hourly)
            space_type: 'source' or 'derived'

        Returns:
            Timeout in seconds (average * multiplier, or fallback minimum)
        """
        lookback_cutoff = datetime.utcnow() - timedelta(days=self.lookback_days)

        # Query: Calculate average duration from completed chunks
        result = session.execute(
            text("""
                SELECT
                    AVG(EXTRACT(EPOCH FROM (completed_at - created_at))) as avg_duration_seconds
                FROM occupancy_space_chunks
                WHERE status = 'COMPLETED'
                  AND space_type = :space_type
                  AND interval_seconds = :interval
                  AND completed_at >= :lookback_cutoff
                  AND completed_at IS NOT NULL
                  AND created_at IS NOT NULL
            """),
            {
                "space_type": space_type,
                "interval": interval_seconds,
                "lookback_cutoff": lookback_cutoff
            }
        ).fetchone()

        avg_duration = result.avg_duration_seconds if result and result.avg_duration_seconds else None

        if avg_duration is None or avg_duration <= 0:
            # No historical data - use minimum fallback
            return self.min_timeout

        # Apply multiplier and round up (convert Decimal to float first)
        calculated_timeout = int(float(avg_duration) * self.multiplier)

        # Ensure at least minimum timeout
        return max(calculated_timeout, self.min_timeout)


# Singleton instance
_timeout_calculator = None

def get_timeout_calculator() -> TimeoutCalculator:
    """Get or create TimeoutCalculator instance."""
    global _timeout_calculator
    if _timeout_calculator is None:
        _timeout_calculator = TimeoutCalculator()
    return _timeout_calculator
