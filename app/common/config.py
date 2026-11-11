from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # shared
    chunk_size: int = 1_048_576
    heartbeat_interval: float = 5.0
    max_concurrent_transfers: int = 8
    log_level: str = "INFO"

    # controller
    database_url: str = "postgresql://objstore:objstore@postgres:5432/objstore"
    controller_port: int = 8080
    staging_dir: str = "/staging"
    heartbeat_timeout: float = 15.0
    meta_cache_enabled: bool = True
    meta_cache_size: int = 1000
    meta_cache_ttl: float = 30.0

    # worker
    worker_id: str = "worker-local"
    worker_address: str = "http://localhost:9000"
    worker_port: int = 9000
    controller_url: str = "http://controller:8080"
    data_dir: str = "/data"
    scrub_interval: float = 1800.0
    chunk_cache_enabled: bool = True
    chunk_cache_bytes: int = 134_217_728


settings = Settings()
