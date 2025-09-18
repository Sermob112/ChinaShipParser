from __future__ import annotations
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


@dataclass
class ChromeDriverFactory:
    user_data_dir: Path
    profile_name: str = "Default"
    headless: bool = False
    binary_path: Optional[Path] = None
    detach: bool = False
    use_profile_clone: bool = False  # <- сразу запускать с клоном профиля

    @staticmethod
    def with_default_windows_profile(profile_name: str = "Default") -> "ChromeDriverFactory":
        local_appdata = os.environ.get("LOCALAPPDATA", "")
        base = Path(local_appdata) / "Google" / "Chrome" / "User Data"
        return ChromeDriverFactory(user_data_dir=base, profile_name=profile_name)

    @staticmethod
    def with_default_yandex_profile(profile_name: str = "Default") -> "ChromeDriverFactory":
        local_appdata = os.environ.get("LOCALAPPDATA", "")
        base = Path(local_appdata) / "Yandex" / "YandexBrowser" / "User Data"
        return ChromeDriverFactory(user_data_dir=base, profile_name=profile_name)

    # ---------- публичный запуск ----------
    def create(self):
        """
        Пробуем старт с оригинальным user-data-dir. Если получаем
        'user data directory is already in use' — создаём временный клон профиля
        и перезапускаем.
        """
        if self.use_profile_clone:
            clone_dir = self._make_profile_clone()
            return self._start_with_user_data_dir(clone_dir, self.profile_name)

        try:
            return self._start_with_user_data_dir(self.user_data_dir, self.profile_name)
        except SessionNotCreatedException as e:
            msg = str(e).lower()
            if "user data directory is already in use" in msg or "please specify a unique value for --user-data-dir" in msg:
                # Автопереход на клон
                clone_dir = self._make_profile_clone()
                return self._start_with_user_data_dir(clone_dir, self.profile_name)
            raise

    # ---------- низкоуровневые части ----------
    def _start_with_user_data_dir(self, user_data_dir: Path, profile_name: str):
        opts = Options()
        opts.add_argument(f"--user-data-dir={str(user_data_dir)}")
        opts.add_argument(f"--profile-directory={profile_name}")

        if self.binary_path:
            opts.binary_location = str(self.binary_path)
        if self.headless:
            opts.add_argument("--headless=new")

        # устойчивость
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-notifications")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--disable-blink-features=AutomationControlled")

        service = Service()
        driver = webdriver.Chrome(service=service, options=opts)

        # cosmetic anti-detect
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            })
        except Exception:
            pass

        if self.detach:
            try:
                driver.execute_cdp_cmd("Browser.setDownloadBehavior", {"behavior": "allow"})
            except Exception:
                pass

        return driver

    def _make_profile_clone(self) -> Path:
        """
        Делаем «лёгкий» клон профиля в %TEMP%/selenium-profile-<ts>.
        Копируем только нужное: профиль и ключевые файлы для сессий/кук.
        НЕ копируем Singleton* (лок-файлы), чтобы не тащить блокировку.
        """
        ts = int(time.time())
        temp_root = Path(tempfile.gettempdir()) / f"selenium-profile-{ts}"
        temp_root.mkdir(parents=True, exist_ok=True)

        # Корневые файлы, которые иногда важны (не критично, best-effort)
        root_files = ["Local State", "Preferences"]
        for name in root_files:
            src = self.user_data_dir / name
            dst = temp_root / name
            if src.exists():
                _safe_copy(src, dst)

        # Копируем ТОЛЬКО нужный профиль (не весь User Data)
        src_profile = self.user_data_dir / self.profile_name
        dst_profile = temp_root / self.profile_name
        if not src_profile.exists():
            raise FileNotFoundError(f"Папка профиля не найдена: {src_profile}")

        dst_profile.mkdir(parents=True, exist_ok=True)

        # Списки директорий/файлов в профиле, которые повышают шанс «перетащить» авторизацию
        # (часть может не существовать — это нормально)
        dirs_to_copy = ["Network", "Service Worker", "Code Cache", "Extension State"]
        files_to_copy = [
            "Preferences",
            "Cookies",           # БД куки (может быть заблокирована — копируем best-effort)
            "Cookies-journal",
            "Login Data",        # сохранённые логины (могут быть зашифрованы DPAPI — это ок)
            "Web Data",
            "History",
            "Favicons",
            "Secure Preferences"
        ]

        for d in dirs_to_copy:
            _safe_copy_dir(src_profile / d, dst_profile / d)

        for f in files_to_copy:
            _safe_copy(src_profile / f, dst_profile / f)

        # Не копируем никакие Singleton*, LOCK, .tmp и т.п.
        return temp_root


def _safe_copy(src: Path, dst: Path):
    try:
        if src.exists() and src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
    except Exception:
        pass  # best-effort

def _safe_copy_dir(src: Path, dst: Path):
    try:
        if src.exists() and src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True, ignore=shutil.ignore_patterns("Singleton*", "LOCK*", "*.tmp"))
    except Exception:
        pass  # best-effort
