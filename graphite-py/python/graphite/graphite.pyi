"""Type stubs for graphite-tsdb."""

from typing import Any, Dict, Optional

import numpy as np
import polars as pl

class DB:
    @staticmethod
    def open(path: str) -> "DB": ...

    def insert(
        self,
        symbol: str,
        timestamp_ns: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ) -> None: ...

    def insert_numpy(
        self,
        symbol: str,
        timestamps: np.ndarray,
        opens: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        volumes: np.ndarray,
    ) -> None: ...

    def query(self, gql: str) -> pl.DataFrame: ...

    def query_range(self, symbol: str, t1: int, t2: int) -> pl.DataFrame: ...

    def compact(self) -> None: ...

    def stats(self) -> Dict[str, Any]: ...

def open(path: str) -> DB: ...
