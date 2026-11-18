from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # shared
    chunk_size: int = 1_048_576
    heartbeat_interval: float = 5.0
    max_concurrent_transfers: int = 8
    max_http_concurrency: int = 200
    log_level: str = "INFO"

    # controller
    database_url: str = "postgresql://objstore:objstore@postgres:5432/objstore"
    controller_port: int = 8080
    staging_dir: str = "/staging"
    heartbeat_timeout: float = 15.0
    meta_cache_enabled: bool = True
    meta_cache_size: int = 1000
    meta_cache_ttl: float = 30.0
    upload_abandon_seconds: float = 3600.0
    max_concurrent_uploads: int = 4
    max_concurrent_downloads: int = 16
    admission_timeout: float = 30.0
    max_upload_bytes: int = 21_474_836_480
    repair_concurrency: int = 2
    repair_scan_interval: float = 10.0
    reconcile_interval: float = 30.0
    orphan_grace_seconds: float = 120.0
    repair_lease_seconds: float = 120.0
    tombstone_retention_seconds: float = 86400.0
    read_hedge_enabled: bool = True
    read_hedge_delay: float = 0.0      # seconds before asking a 2nd replica; 0 = adaptive (p95 of recent reads)
    read_hedge_min_delay: float = 0.02
    read_hedge_max_delay: float = 1.0
    read_ahead_chunks: int = 2         # chunks fetched ahead of the one being sent to the client
    rebalance_enabled: bool = True
    rebalance_interval: float = 15.0
    rebalance_tolerance: int = 2     # allowed difference in replica count between the fullest and emptiest worker
    rebalance_batch: int = 16        # replicas moved per round at most
    rebalance_concurrency: int = 2

    # worker
    worker_id: str = "worker-local"
    worker_address: str = "http://localhost:9000"
    worker_port: int = 9000
    controller_url: str = "http://controller:8080"
    data_dir: str = "/data"
    scrub_interval: float = 1800.0
    scrub_rate_mb_s: float = 25.0
    scrub_missing_min_age: float = 30.0
    chunk_cache_enabled: bool = True
    chunk_cache_bytes: int = 134_217_728


settings = Settings()
