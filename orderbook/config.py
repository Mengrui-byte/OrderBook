"""Configuration loading for the order book package."""
from configparser import ConfigParser
from dataclasses import dataclass
import os


_DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.ini")


def _path(value):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(value)))


@dataclass(frozen=True)
class Config:
    config_path: str
    tardis_root: str
    checkpoint_root: str
    ts_divisor: int
    crossover_log_threshold: int
    cache_size: int
    io_workers: int
    market: str
    workers: int


def load_config(path=None):
    """Load configuration from ``path`` or ORDERBOOK_CONFIG/default file."""
    config_path = _path(path or os.environ.get("ORDERBOOK_CONFIG", _DEFAULT_CONFIG_PATH))
    parser = ConfigParser()
    if not parser.read(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    paths = parser["paths"]
    defaults = parser["defaults"]
    return Config(
        config_path=config_path,
        tardis_root=_path(paths["tardis_root"]),
        checkpoint_root=_path(paths["checkpoint_root"]),
        ts_divisor=defaults.getint("ts_divisor"),
        crossover_log_threshold=defaults.getint("crossover_log_threshold"),
        cache_size=defaults.getint("cache_size"),
        io_workers=defaults.getint("io_workers"),
        market=defaults.get("market").lower(),
        workers=defaults.getint("workers"),
    )


CONFIG = load_config()

