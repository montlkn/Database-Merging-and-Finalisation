"""
Shared utility functions for NYC Buildings Pipeline
"""

import pandas as pd
from typing import Optional, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_dataframe(df: pd.DataFrame, required_columns: list[str]) -> None:
    """
    Validate that dataframe has required columns.
    Raises ValueError if columns are missing.
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    logger.info(f"✓ Validated {len(df)} rows with required columns")


def validate_year(year: Optional[float], min_year: int = 1850, max_year: int = 2025) -> bool:
    """Validate year is in reasonable range."""
    if pd.isna(year):
        return False
    return min_year <= year <= max_year


def validate_floors(floors: Optional[float], min_floors: int = 1, max_floors: int = 180) -> bool:
    """Validate number of floors is reasonable."""
    if pd.isna(floors):
        return False
    return min_floors <= floors <= max_floors


def safe_float(value, default: float = None) -> Optional[float]:
    """Safely convert value to float, return default if fails."""
    try:
        return float(value) if pd.notna(value) else default
    except (ValueError, TypeError):
        return default


def safe_int(value, default: int = None) -> Optional[int]:
    """Safely convert value to int, return default if fails."""
    try:
        return int(value) if pd.notna(value) else default
    except (ValueError, TypeError):
        return default


def parse_point(geom_str: str) -> Optional[Tuple[float, float]]:
    """
    Parse POINT(-73.9715 40.7588) string to (lng, lat) tuple.
    Returns None if parsing fails.
    """
    try:
        if not geom_str or not geom_str.startswith('POINT'):
            return None

        # Extract coordinates from POINT(lng lat)
        coords = geom_str.replace('POINT(', '').replace(')', '').strip()
        lng, lat = map(float, coords.split())
        return (lng, lat)
    except Exception as e:
        logger.warning(f"Failed to parse POINT geometry: {geom_str} - {e}")
        return None


def save_checkpoint(df: pd.DataFrame, filepath: str, description: str = "") -> None:
    """Save intermediate checkpoint with logging."""
    df.to_csv(filepath, index=False)
    logger.info(f"✓ Saved checkpoint: {filepath} ({len(df)} rows) {description}")


def load_checkpoint(filepath: str) -> pd.DataFrame:
    """Load checkpoint with logging."""
    df = pd.read_csv(filepath)
    logger.info(f"✓ Loaded checkpoint: {filepath} ({len(df)} rows)")
    return df
