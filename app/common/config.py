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
