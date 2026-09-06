"""
Single source of truth for ALL system configuration.

Every worker (surface, deep, dark, llm), every service, and every
integration test reads configuration from ``settings``.

Scalar values can be overridden via environment variables or ``.env``.
Complex values (headers, chromium args, stealth script) are class
attributes — override them in ``__init__`` or subclass ``Settings``.

Usage::

    from app.common.config.settings import settings
    max_depth = settings.CRAWL_MAX_DEPTH
    ua = settings.SURFACE_DEFAULT_HEADERS["User-Agent"]
"""

import os
import secrets

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ==================================================================
    # Project Metadata
    # ==================================================================
    PROJECT_NAME: str = "DukaScraper"
    VERSION: str = "1.0.0"
    PROJECT_VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    # ==================================================================
    # Security
    # ==================================================================
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    PASSWORD_RESET_EXPIRE_MINUTES: int = 30
    PASSWORD_RESET_URL: str = "http://localhost:5173/reset-password"
    EMAIL_VERIFICATION_URL: str = "http://localhost:5173/verify-email"
    LOGIN_RATE_LIMIT_PER_MINUTE: int = 10
    RESET_RATE_LIMIT_PER_HOUR: int = 5

    # Bootstrap admin (created on first run)
    INITIAL_ADMIN_USERNAME: str = "dukaadmin"
    INITIAL_ADMIN_EMAIL: str = ""
    INITIAL_ADMIN_PASSWORD: str = ""
    INITIAL_ADMIN_NAME: str = "System Administrator"

    # SMTP (Gmail or other provider)
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_EMAIL: str = ""
    SMTP_USE_TLS: bool = True

    # ==================================================================
    # CORS
    # ==================================================================
    BACKEND_CORS_ORIGINS: list[str] = ["*"]

    # ==================================================================
    # Crawl Pipeline (shared by ALL workers)
    # ==================================================================
    CRAWL_MAX_DEPTH: int = 10
    CRAWL_MAX_LINKS_PER_PAGE: int = 25
    CRAWL_ENABLE_EXTRACTION: bool = True
    CRAWL_SAME_DOMAIN_ONLY: bool = True
    CRAWL_AUTO_EXPAND_DOMAINS: list[str] = []
    OBEY_ROBOTS_TXT: bool = False

    # ==================================================================
    # Content Deduplication (3-tier)
    # ==================================================================
    DEDUP_ENABLED: bool = True
    DEDUP_SIMHASH_THRESHOLD: int = 5  # Hamming distance for near-duplicates (3=strict, 5=balanced, 7=relaxed)
    DEDUP_STALE_HOURS: float = 24.0   # Re-fetch content older than this
    deep_max_pages_per_job: int = 50
    dark_enabled: bool = True
    tor_proxy_url: str = "socks5://tor:9050"
    proxy_pool: str = ""
    RESIDENTIAL_PROXY: str = ""  # e.g. socks5://user:pass@host:port
    export_batch_size: int = 100
    export_flush_interval_seconds: int = 60
    http_timeout_seconds: float = 30.0
    deep_timeout_seconds: float = 45.0
    dark_timeout_seconds: float = 60.0
    tor_max_concurrency: int = 10

    # ==================================================================
    # Surface Worker
    # ==================================================================
    SURFACE_MAX_CONCURRENT_TASKS: int = 50

    # ==================================================================
    # Deep Worker (Browser / Patchright)
    # ==================================================================
    DEEP_MAX_CONCURRENT_JOBS: int = 5

    # ==================================================================
    # Dark Worker (Tor)
    # ==================================================================
    DARK_MAX_CONCURRENT_TASKS: int = 10
    DARK_FETCH_RETRIES: int = 2
    DARK_MAX_RESPONSE_BYTES: int = 20 * 1024 * 1024  # 20 MB
    DARK_CONNECT_TIMEOUT: float = 60.0
    DARK_READ_TIMEOUT: float = 60.0
    DARK_WRITE_TIMEOUT: float = 60.0
    DARK_POOL_TIMEOUT: float = 60.0

    # ==================================================================
    # LLM Worker
    # ==================================================================
    LLM_MAX_CONCURRENT_TASKS: int = 4

    # ==================================================================
    # Auth Session (optional)
    # ==================================================================
    AUTH_SESSION_STATE_PATH: str = ""
    AUTH_SESSION_ALLOWED_HOSTS: str = ""

    # ==================================================================
    # Gmail OAuth2 (default credentials for auto signup/login verification)
    # ==================================================================
    # Per-credential Gmail OAuth overrides these when set on the credential row.
    GMAIL_CLIENT_ID: str = ""
    GMAIL_CLIENT_SECRET: str = ""
    GMAIL_REFRESH_TOKEN: str = ""

    # ==================================================================
    # IMAP (backup for email verification — used when OAuth2 is unavailable)
    # ==================================================================
    IMAP_HOST: str = "imap.gmail.com"
    IMAP_PORT: int = 993
    IMAP_USERNAME: str = ""
    IMAP_PASSWORD: str = ""
    IMAP_USE_SSL: bool = True
    # The email address to read verification codes FROM
    SEED_GMAIL_EMAIL: str = ""
    SEED_GMAIL_PASSWORD: str = ""

    # ==================================================================
    # Kafka Topics
    # ==================================================================
    crawl_request_topic: str = "crawl.requests"
    crawl_raw_topic: str = "crawl.raw"
    crawl_parsed_topic: str = "crawl.parsed"

    # ==================================================================
    # MinIO Storage
    # ==================================================================
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_RAW_BUCKET: str = "duka-raw-data"
    MINIO_PARSED_BUCKET: str = "duka-parsed-data"

    # ==================================================================
    # ClickHouse
    # ==================================================================
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_HTTP_PORT: int = 8123
    CLICKHOUSE_NATIVE_PORT: int = 9002
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "duka_scraper"

    # ==================================================================
    # PostgreSQL
    # ==================================================================
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "duka"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    DUKA_SYSTEM_DB: str = "duka_system"

    # ==================================================================
    # Infrastructure Connections
    # ==================================================================
    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    ELASTICSEARCH_URL: str = "http://localhost:9200"

    # ==================================================================
    # Monitoring tools (browser URLs surfaced by GET /api/v1/monitoring/urls)
    # These point at the docker-compose host port mappings; override per env.
    # ==================================================================
    GRAFANA_URL: str = "http://localhost:3000"
    PROMETHEUS_URL: str = "http://localhost:9090"
    KAFKA_UI_URL: str = "http://localhost:8088"
    KIBANA_URL: str = "http://localhost:5601"
    PGADMIN_URL: str = "http://localhost:5050"
    MINIO_CONSOLE_URL: str = "http://localhost:9001"

    # ==================================================================
    # Integration Test Defaults
    # ==================================================================
    TEST_API_BASE_URL: str = "http://localhost:8000"
    TEST_USER_ID: str = "USR99999"
    TEST_POLL_INTERVAL: int = 5
    TEST_MAX_WAIT_SECONDS: int = 300

    # ==================================================================
    # Complex defaults (not from env — override in __init__ or subclass)
    # ==================================================================

    SURFACE_DEFAULT_HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    DARK_DEFAULT_HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
    }

    DEEP_CHROMIUM_ARGS: list[str] = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-ipv6",
        "--dns-prefetch-disable",
        "--ignore-certificate-errors",
        "--font-render-hinting=none",
        "--disable-gpu-rasterization",
        "--use-gl=swiftshader",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-extensions",
        "--disable-sync",
        "--no-first-run",
    ]

    DEEP_DEFAULT_USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    )

    DEEP_BROWSER_CONTEXT_KWARGS: dict[str, object] = {
        "viewport": {"width": 1920, "height": 1080},
        "locale": "en-US",
        "timezone_id": "America/New_York",
        "extra_http_headers": {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
            "Sec-Ch-Ua": (
                '"Chromium";v="128", "Not;A=Brand";v="24", '
                '"Google Chrome";v="128"'
            ),
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Upgrade-Insecure-Requests": "1",
        },
    }

    # Injected BEFORE page scripts to defeat Cloudflare / bot-detection.
    DEEP_STEALTH_INIT_SCRIPT: str = """
(() => {
    // ---- 1. Navigator & DOM API signals ----
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true,
    });
    delete navigator.__proto__.webdriver;

    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true,
    });
    Object.defineProperty(navigator, 'language', {
        get: () => 'en-US',
        configurable: true,
    });
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8,
        configurable: true,
    });
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8,
        configurable: true,
    });
    Object.defineProperty(navigator, 'maxTouchPoints', {
        get: () => 0,
        configurable: true,
    });
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Win32',
        configurable: true,
    });
    if (!navigator.connection) {
        Object.defineProperty(navigator, 'connection', {
            get: () => ({
                effectiveType: '4g',
                rtt: 50,
                downlink: 10,
                saveData: false,
            }),
            configurable: true,
        });
    }
    const nativeToString = Function.prototype.toString;
    Function.prototype.toString = function() {
        if (this === navigator.permissions.query) {
            return 'function query() { [native code] }';
        }
        return nativeToString.call(this);
    };

    // ---- 2. WebGL / Canvas / AudioContext fingerprinting noise ----
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function() {
        if (this.width === 16 && this.height === 16) {
            return origToDataURL.apply(this, arguments);
        }
        const ctx = this.getContext('2d');
        if (ctx) {
            const pixel = ctx.getImageData(0, 0, 1, 1);
            pixel.data[0] = pixel.data[0] ^ 1;
            ctx.putImageData(pixel, 0, 0);
        }
        return origToDataURL.apply(this, arguments);
    };
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel Iris OpenGL Engine';
        return getParameter.call(this, param);
    };
    const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel Iris OpenGL Engine';
        return getParameter2.call(this, param);
    };
    if (window.AudioContext || window.webkitAudioContext) {
        const AC = window.AudioContext || window.webkitAudioContext;
        const origCreateOscillator = AC.prototype.createOscillator;
        AC.prototype.createOscillator = function() {
            const osc = origCreateOscillator.call(this);
            const origGetFloatFrequencyData = osc.frequency?.getFloatFrequencyData;
            if (origGetFloatFrequencyData) {
                osc.frequency.getFloatFrequencyData = function(arr) {
                    origGetFloatFrequencyData.call(this, arr);
                    arr[0] += 0.001;
                };
            }
            return osc;
        };
    }

    // ---- 3. Browser object structure ----
    if (!window.chrome) {
        window.chrome = {};
    }
    if (!window.chrome.app) {
        window.chrome.app = {
            isInstalled: false,
            InstallState: {
                DISABLED: 'disabled',
                INSTALLED: 'installed',
                NOT_INSTALLED: 'not_installed',
            },
            RunningState: {
                CANNOT_RUN: 'cannot_run',
                READY_TO_RUN: 'ready_to_run',
                RUNNING: 'running',
            },
        };
    }
    if (!window.chrome.runtime) {
        window.chrome.runtime = {
            PlatformOs: {
                MAC: 'mac', WIN: 'win', ANDROID: 'android',
                CROS: 'cros', LINUX: 'linux', OPENBSD: 'openbsd',
            },
            PlatformArch: {
                ARM: 'arm', X86_32: 'x86-32', X86_64: 'x86-64',
            },
            connect: function() {},
            sendMessage: function() {},
        };
    }
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                { name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                { name: 'Microsoft Edge PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                { name: 'WebKit built-in PDF', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
            ];
            plugins.length = 5;
            plugins.item = (i) => plugins[i];
            plugins.namedItem = (n) => plugins.find(p => p.name === n);
            plugins.refresh = () => {};
            return plugins;
        },
        configurable: true,
    });
    Object.defineProperty(navigator, 'mimeTypes', {
        get: () => {
            const mimes = [{ type: 'application/pdf', suffixes: '', description: 'Portable Document Format' }];
            mimes.length = 1;
            mimes.item = (i) => mimes[i];
            mimes.namedItem = (n) => mimes.find(m => m.type === n);
            return mimes;
        },
        configurable: true,
    });

    // ---- 4. Permissions & API availability ----
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (params) => {
        const defaults = {
            notifications: Notification.permission,
            geolocation: 'prompt',
            camera: 'prompt',
            microphone: 'prompt',
            'speaker-selection': 'prompt',
            'device-info': 'prompt',
            'background-fetch': 'prompt',
            'background-sync': 'granted',
            bluetooth: 'prompt',
            'persistent-storage': 'prompt',
            'periodic-background-sync': 'prompt',
            'screen-wake-lock': 'prompt',
            'nfc': 'prompt',
        };
        if (params.name in defaults) {
            return Promise.resolve({ state: defaults[params.name] });
        }
        return origQuery(params);
    };
    if (!navigator.geolocation) {
        navigator.geolocation = {
            getCurrentPosition: (success) => success({
                coords: { latitude: 40.7128, longitude: -74.0060, accuracy: 10 },
                timestamp: Date.now(),
            }),
            watchPosition: () => 1,
            clearWatch: () => {},
        };
    }

    // ---- 5. Screen & viewport consistency ----
    Object.defineProperty(screen, 'colorDepth', { get: () => 24, configurable: true });
    Object.defineProperty(screen, 'pixelDepth', { get: () => 24, configurable: true });

    // ---- 6. CDP / automation residual markers ----
    for (const key of [
        '__webdriver_evaluate', '__webdriver_script_function',
        '__webdriver_script_func', '__webdriver_script_fn',
        '__fxdriver_evaluate', '__driver_unwrapped',
        '__webdriver_unwrapped', '__driver_evaluate',
        'callPhantom', '_phantom', '__nightmare',
        '_selenium', 'callSelenium', '_Selenium_IDE_Recorder',
        '__webdriver_evaluate__',
    ]) {
        delete window[key];
    }
    const origGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
    Object.getOwnPropertyDescriptor = function(obj, prop) {
        if (prop === 'webdriver' && obj === navigator) {
            return undefined;
        }
        return origGetOwnPropertyDescriptor.call(this, obj, prop);
    };
})();
"""

    # ==================================================================
    # Lifecycle
    # ==================================================================

    @staticmethod
    def _detect_wsl() -> bool:
        """Auto-detect WSL2 environment via multiple signals."""
        if os.getenv("APP_ENV") == "wsl":
            return True
        # Check 1: /proc/version (reliable on most WSL2 distros)
        try:
            with open("/proc/version") as f:
                version = f.read().lower()
                if "microsoft" in version or "wsl" in version:
                    return True
        except (FileNotFoundError, PermissionError):
            pass
        # Check 2: os.uname().release often contains 'microsoft' on WSL
        try:
            release = os.uname().release.lower()
            if "microsoft" in release or "wsl" in release:
                return True
        except Exception:
            pass
        # Check 3: WSLInterop environment variable (set by most WSL distros)
        if os.getenv("WSL_DISTRO_NAME") or os.getenv("WSL_INTEROP"):
            return True
        # Check 4: /proc/sys/fs/binfmt_misc/WSLInterop exists
        if os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop"):
            return True
        return False

    @staticmethod
    def _detect_docker() -> bool:
        """Auto-detect Docker environment."""
        if os.getenv("APP_ENV") == "docker":
            return True
        if os.path.exists("/.dockerenv"):
            return True
        try:
            with open("/proc/1/cgroup") as f:
                return "docker" in f.read().lower()
        except (FileNotFoundError, PermissionError):
            pass
        return False

    def __init__(self, **data):
        super().__init__(**data)

        # Generate JWT_SECRET_KEY if not provided
        if not self.JWT_SECRET_KEY:
            self.JWT_SECRET_KEY = secrets.token_urlsafe(32)

        # Inject user_agent into browser context kwargs
        if "user_agent" not in self.DEEP_BROWSER_CONTEXT_KWARGS:
            self.DEEP_BROWSER_CONTEXT_KWARGS["user_agent"] = self.DEEP_DEFAULT_USER_AGENT

        # --- Auto-detect environment ---
        if self._detect_docker():
            self.MINIO_ENDPOINT = "minio:9000"
            self.CLICKHOUSE_HOST = "clickhouse"
            self.POSTGRES_HOST = "postgres"
            self.REDIS_URL = "redis://redis:6379/0"
            self.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
            self.ELASTICSEARCH_URL = "http://elasticsearch:9200"
        elif self._detect_wsl():
            # WSL2: services exposed via Docker Desktop port mappings on localhost
            import logging as _log
            _log.getLogger("duka.config").info(
                "WSL2 environment detected — rewriting service endpoints to localhost ports"
            )
            # IMPORTANT: Do NOT use os.getenv() for Kafka — the .env file often
            # sets KAFKA_BOOTSTRAP_SERVERS=kafka:9092 (Docker internal), which
            # doesn't resolve from WSL.  Always force the EXTERNAL listener.
            self.KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
            self.MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
            self.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            self.POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
            self.POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
            self.CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
            self.CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
            self.CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9002"))
            self.ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
            self.tor_proxy_url = os.getenv("tor_proxy_url", "socks5://localhost:9050")

        # --- Failsafe: always use EXTERNAL Kafka listener (29092) outside Docker ---
        # If KAFKA_BOOTSTRAP_SERVERS points to port 9092 (internal Docker listener),
        # the broker will advertise 'kafka:9092' which doesn't resolve from WSL.
        # Force port 29092 (EXTERNAL listener) which advertises 'localhost:29092'.
        _kafka_bs = self.KAFKA_BOOTSTRAP_SERVERS
        if not self._detect_docker() and (":9092" in _kafka_bs and ":29092" not in _kafka_bs):
            import logging as _log2
            _log2.getLogger("duka.config").warning(
                "Kafka bootstrap '%s' uses port 9092 (Docker internal). "
                "Forcing port 29092 (EXTERNAL listener) for non-Docker environment.",
                _kafka_bs,
            )
            self.KAFKA_BOOTSTRAP_SERVERS = _kafka_bs.replace(":9092", ":29092")

    def validate_config(self) -> list[str]:
        """Validate configuration and return any warnings."""
        warnings = []

        if self.POSTGRES_HOST == "localhost" and os.getenv("APP_ENV") == "docker":
            warnings.append("POSTGRES_HOST is localhost in docker environment")

        if self.MINIO_ROOT_USER == "minioadmin":
            warnings.append("Using default MinIO credentials — change in production")

        if not self.KAFKA_BOOTSTRAP_SERVERS or self.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092":
            if os.getenv("APP_ENV") == "docker":
                warnings.append("KAFKA_BOOTSTRAP_SERVERS points to localhost in docker environment")

        if self.BACKEND_CORS_ORIGINS == ["*"]:
            warnings.append("CORS is configured to allow all origins — restrict in production")

        return warnings

    @property
    def SECRET_KEY(self) -> str:
        """Backward-compatible alias — prefer JWT_SECRET_KEY in new code."""
        return self.JWT_SECRET_KEY

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.DUKA_SYSTEM_DB}"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
