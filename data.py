import logging
import os
import sys
import time
import uuid
from datetime import datetime

import pandas as pd
import requests
from PySide6.QtCore import QDate, QThread, Signal, QTimer, Qt
from PySide6.QtGui import QBrush, QColor, QFont, QIcon, QLinearGradient, QPainter, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDialog,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplashScreen,
    QStatusBar,
    QVBoxLayout,
    QWidget,
    QDialogButtonBox,
)

try:
    from main import Interval, TvDatafeed
except ImportError:
    print("Error: Could not import TvDatafeed and Interval from main_backup.py")
    print("Please ensure main_backup.py is in the same directory or install tvdatafeed")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Exchange mapping
EXCHANGE_MAP = {
    "NSE": {"region": "india", "market": "nse"},
    "AMEX": {"region": "america", "market": "amex"},
    "SNP 500": {"region": "america", "markets": ["nyse", "nasdaq"]},
}

def get_symbols(exchange):
    """Fetch symbols for the given exchange from TradingView API."""
    try:
        if exchange not in EXCHANGE_MAP:
            raise ValueError(f"Exchange {exchange} not supported")

        if exchange == "SNP 500":
            symbols = set()
            for market in EXCHANGE_MAP[exchange]["markets"]:
                url = f"https://scanner.tradingview.com/{EXCHANGE_MAP[exchange]['region']}/scan"
                payload = {
                    "filter": [],
                    "options": {"lang": "en"},
                    "markets": [market],
                    "symbols": {"query": {"types": []}, "tickers": []},
                    "columns": ["name"],
                }
                headers = {
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
                }
                response = requests.post(url, json=payload, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                market_symbols = {item["d"][0] for item in data.get("data", []) if item["d"][0]}
                symbols.update(market_symbols)
            return sorted(list(symbols))
        else:
            region = EXCHANGE_MAP[exchange]["region"]
            market = EXCHANGE_MAP[exchange]["market"]
            url = f"https://scanner.tradingview.com/{region}/scan"
            payload = {
                "filter": [],
                "options": {"lang": "en"},
                "markets": [market],
                "symbols": {"query": {"types": []}, "tickers": []},
                "columns": ["name"],
            }
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
            }
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            return sorted(list(set(item["d"][0] for item in data.get("data", []) if item["d"][0])))
    except Exception as e:
        logger.error(f"Error fetching symbols for {exchange}: {str(e)}")
        return []

class SymbolFetchThread(QThread):
    symbols_fetched = Signal(list)
    error = Signal(str)

    def __init__(self, exchange):
        super().__init__()
        self.exchange = exchange

    def run(self):
        symbols = get_symbols(self.exchange)
        if symbols:
            self.symbols_fetched.emit(symbols)
        else:
            self.error.emit(f"No symbols returned for {self.exchange}")

class LoadingScreen(QSplashScreen):
    def __init__(self):
        pixmap = QPixmap(400, 300)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        gradient = QLinearGradient(0, 0, 0, 300)
        gradient.setColorAt(0, QColor("#1e293b"))
        gradient.setColorAt(1, QColor("#0f172a"))
        painter.fillRect(pixmap.rect(), gradient)
        painter.setPen(QColor("#f1f5f9"))
        painter.setFont(QFont("Inter", 32, QFont.Bold))
        title_rect = pixmap.rect().adjusted(0, -40, 0, 0)
        painter.drawText(title_rect, Qt.AlignCenter, "Data Downloader")
        painter.setPen(QColor("#60a5fa"))
        painter.setFont(QFont("Inter", 14, QFont.Medium))
        painter.drawText(20, 270, "Initializing...")
        painter.end()
        super().__init__(pixmap)
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_loading_text)
        self.timer.start(500)
        self.dots = 0

    def update_loading_text(self):
        self.dots = (self.dots + 1) % 4
        dots = "." * self.dots
        pixmap = self.pixmap()
        painter = QPainter(pixmap)
        painter.fillRect(20, 250, 200, 30, Qt.transparent)
        painter.setPen(QColor("#60a5fa"))
        painter.setFont(QFont("Inter", 14, QFont.Medium))
        painter.drawText(20, 270, f"Initializing{dots}")
        painter.end()
        self.setPixmap(pixmap)

class DataFetchThread(QThread):
    data_fetched = Signal(dict)
    error = Signal(str)
    progress = Signal(int)
    log_message = Signal(str, str)

    def __init__(self, tv, symbols, exchange, interval, n_bars, max_retries=3):
        super().__init__()
        self.tv = tv
        self.symbols = symbols
        self.exchange = exchange
        self.interval = interval
        self.n_bars = n_bars
        self.max_retries = max_retries

    def run(self):
        try:
            results = {}
            failed_symbols = []
            total_symbols = len(self.symbols)

            for idx, symbol in enumerate(self.symbols):
                success = False
                for attempt in range(self.max_retries):
                    self.log_message.emit(
                        f"Fetching {self.n_bars} rows for {symbol} (Attempt {attempt + 1}/{self.max_retries})",
                        "#60a5fa",
                    )
                    try:
                        df = self.tv.get_hist(
                            symbol=symbol,
                            exchange=self.exchange,
                            interval=self.interval,
                            n_bars=self.n_bars,
                        )
                        if df is not None and not df.empty:
                            df = df.reset_index()
                            if "timestamp" in df.columns:
                                df = df.rename(columns={"timestamp": "datetime"})
                            if "symbol" in df.columns:
                                df["symbol"] = df["symbol"].str.replace(f"{self.exchange}:", "", regex=False)
                            results[symbol] = df
                            self.log_message.emit(f"Downloaded {len(df)} rows for {symbol}", "#22c55e")
                            success = True
                            break
                        else:
                            self.log_message.emit(f"No data for {symbol} (Attempt {attempt + 1})", "#ef4444")
                    except Exception as e:
                        self.log_message.emit(f"Error fetching {symbol} (Attempt {attempt + 1}): {str(e)}", "#ef4444")
                    time.sleep(1)

                if not success:
                    failed_symbols.append(symbol)
                    self.log_message.emit(f"Failed to download {symbol} after {self.max_retries} attempts", "#ef4444")

                self.progress.emit(int(((idx + 1) / total_symbols) * 100))

            retry_round = 1
            while failed_symbols and retry_round <= self.max_retries:
                self.log_message.emit(
                    f"Retry round {retry_round} for {len(failed_symbols)} symbols: {', '.join(failed_symbols)}",
                    "#f59e0b",
                )
                new_failed = []
                for symbol in failed_symbols:
                    success = False
                    for attempt in range(self.max_retries):
                        self.log_message.emit(
                            f"Retrying {symbol} (Round {retry_round}, Attempt {attempt + 1}/{self.max_retries})",
                            "#60a5fa",
                        )
                        try:
                            df = self.tv.get_hist(
                                symbol=symbol,
                                exchange=self.exchange,
                                interval=self.interval,
                                n_bars=self.n_bars,
                            )
                            if df is not None and not df.empty:
                                df = df.reset_index()
                                if "timestamp" in df.columns:
                                    df = df.rename(columns={"timestamp": "datetime"})
                                if "symbol" in df.columns:
                                    df["symbol"] = df["symbol"].str.replace(f"{self.exchange}:", "", regex=False)
                                results[symbol] = df
                                self.log_message.emit(f"Downloaded {len(df)} rows for {symbol} on retry", "#22c55e")
                                success = True
                                break
                            else:
                                self.log_message.emit(f"No data for {symbol} (Retry attempt {attempt + 1})", "#ef4444")
                        except Exception as e:
                            self.log_message.emit(f"Error retrying {symbol} (Attempt {attempt + 1}): {str(e)}", "#ef4444")
                        time.sleep(1)

                    if not success:
                        new_failed.append(symbol)

                failed_symbols = new_failed
                retry_round += 1

            if failed_symbols:
                self.log_message.emit(f"Final failures: {', '.join(failed_symbols)}", "#ef4444")
            else:
                self.log_message.emit(f"All {total_symbols} symbols fetched successfully", "#22c55e")

            self.data_fetched.emit(results)
        except Exception as e:
            self.error.emit(str(e))

class ModernButton(QPushButton):
    def __init__(self, text, primary=True):
        super().__init__(text)
        self.primary = primary
        self.setFixedHeight(50)
        self.setCursor(Qt.PointingHandCursor)
        self.update_style()
        if primary:
            self.setIcon(QIcon.fromTheme("system-search"))
        else:
            self.setIcon(QIcon.fromTheme("document-save"))

    def update_style(self):
        if self.primary:
            self.setStyleSheet("""
                QPushButton {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3b82f6, stop:1 #2563eb);
                    color: #ffffff;
                    border: none;
                    border-radius: 12px;
                    font-size: 16px;
                    font-weight: 600;
                    font-family: Inter;
                    padding: 12px 24px;
                    icon-size: 22px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                QPushButton:hover {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #60a5fa, stop:1 #3b82f6);
                }
                QPushButton:pressed {
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2563eb, stop:1 #1d4ed8);
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
                }
                QPushButton:disabled {
                    background: #6b7280;
                    color: #d1d5db;
                    box-shadow: none;
                }
            """)
        else:
            self.setStyleSheet("""
                QPushButton {
                    background: #1e293b;
                    color: #60a5fa;
                    border: 1px solid #475569;
                    border-radius: 12px;
                    font-size: 16px;
                    font-weight: 600;
                    font-family: Inter;
                    padding: 12px 24px;
                    icon-size: 22px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
                QPushButton:hover {
                    background: #334155;
                    border-color: #64748b;
                }
                QPushButton:pressed {
                    background: #475569;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
                }
                QPushButton:disabled {
                    background: #1e293b;
                    color: #9ca3af;
                    border-color: #475569;
                    box-shadow: none;
                }
            """)

class SymbolSelectionDialog(QDialog):
    def __init__(self, parent, symbols, initial_selected_symbols=None):
        super().__init__(parent)
        self.setWindowTitle("Select Symbols")
        self.setMinimumSize(600, 500)
        self.items = sorted(symbols)  # Available symbols from the exchange
        self.selected_items = initial_selected_symbols if initial_selected_symbols else []
        self.parent = parent
        self.init_ui()

    def init_ui(self):
        self.setStyleSheet("""
            QDialog {
                background: #1e293b;
                color: #e2e8f0;
                font-family: Inter;
            }
            QLabel {
                color: #e2e8f0;
                font-size: 16px;
                font-weight: 400;
            }
            QLineEdit {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                padding: 12px 16px;
                color: #e2e8f0;
                font-size: 16px;
                font-family: Inter;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QLineEdit:focus {
                border-color: #60a5fa;
                box-shadow: 0 0 0 3px rgba(96, 165, 250, 0.2);
                outline: none;
            }
            QLineEdit::placeholder {
                color: #94a3b8;
            }
            QListWidget {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                color: #e2e8f0;
                font-size: 15px;
                padding: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QListWidget::item {
                background: transparent;
                padding: 6px 8px;
                border-bottom: 1px solid #475569;
            }
            QListWidget::item:last-child {
                border-bottom: none;
            }
            QScrollBar:vertical {
                background: transparent;
                width: 12px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical {
                background: #64748b;
                border-radius: 6px;
                min-height: 20px;
                margin: 2px;
            }
            QScrollBar::handle:vertical:hover {
                background: #94a3b8;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QDialogButtonBox QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3b82f6, stop:1 #2563eb);
                color: #ffffff;
                border: none;
                border-radius: 12px;
                padding: 10px 20px;
                font-weight: 600;
                min-width: 80px;
                font-size: 16px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QDialogButtonBox QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #60a5fa, stop:1 #3b82f6);
            }
            QDialogButtonBox QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2563eb, stop:1 #1d4ed8);
            }
        """)

        layout = QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)
        self.setLayout(layout)

        label = QLabel("Select Symbols")
        label.setStyleSheet("font-size: 16px; font-weight: 500; color: #94a3b8; margin-bottom: 4px;")
        layout.addWidget(label)

        self.selected_label = QLabel(", ".join(self.selected_items) if self.selected_items else "No symbols selected")
        self.selected_label.setStyleSheet("""
            QLabel {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                padding: 12px 16px;
                color: #e2e8f0;
                font-size: 16px;
                font-family: Inter;
                min-height: 50px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
        """)
        self.selected_label.setWordWrap(True)
        layout.addWidget(self.selected_label)

        self.search_input = QLineEdit()
        self.search_input.setFixedHeight(50)
        self.search_input.setPlaceholderText("Type to search symbols...")
        layout.addWidget(self.search_input)

        self.list_widget = QListWidget()
        self.list_widget.setMinimumHeight(200)
        self.list_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.list_widget.setFocusPolicy(Qt.NoFocus)
        self.list_widget.setHidden(True)
        layout.addWidget(self.list_widget)

        button_layout = QHBoxLayout()
        self.select_all_button = ModernButton("Select All", primary=False)
        self.clear_all_button = ModernButton("Clear All", primary=False)
        self.select_all_button.clicked.connect(self.select_all_symbols)
        self.clear_all_button.clicked.connect(self.clear_all_symbols)
        button_layout.addWidget(self.select_all_button)
        button_layout.addWidget(self.clear_all_button)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Dialog buttons (OK/Cancel)
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.all_checkboxes = []
        for item in self.items:
            list_item = QListWidgetItem()
            checkbox = QCheckBox(item)
            checkbox.setStyleSheet("""
                QCheckBox {
                    background: transparent;
                    color: #e2e8f0;
                    font-size: 15px;
                    border: none;
                    spacing: 12px;
                }
                QCheckBox::indicator {
                    width: 20px;
                    height: 20px;
                    background: #1e293b;
                    border: 1px solid #475569;
                    border-radius: 6px;
                }
                QCheckBox::indicator:checked {
                    background: #2563eb;
                    border-color: #2563eb;
                }
                QCheckBox::indicator:checked:hover {
                    background: #3b82f6;
                }
            """)
            checkbox.setChecked(item in self.selected_items)
            checkbox.stateChanged.connect(self.on_checkbox_changed)
            self.list_widget.addItem(list_item)
            self.list_widget.setItemWidget(list_item, checkbox)
            self.all_checkboxes.append((list_item, checkbox))

        self.search_input.textChanged.connect(self.on_search_text_changed)
        self.search_input.focusInEvent = lambda event: self.show_dropdown(event)
        self.search_input.editingFinished.connect(self.hide_dropdown)

    def show_dropdown(self, event):
        if self.list_widget.count() > 0:
            self.list_widget.setHidden(False)

    def hide_dropdown(self):
        if not self.search_input.hasFocus() and not self.list_widget.hasFocus():
            self.list_widget.setHidden(True)

    def on_search_text_changed(self, text):
        self.filter_symbols(text)
        if self.list_widget.count() > 0 and any(
            not self.list_widget.item(i).isHidden() for i in range(self.list_widget.count())
        ):
            self.list_widget.setHidden(False)
        else:
            self.list_widget.setHidden(True)

    def filter_symbols(self, text):
        text = text.lower().strip()
        for list_item, checkbox in self.all_checkboxes:
            symbol = checkbox.text().lower()
            list_item.setHidden(text != "" and text not in symbol)

    def on_checkbox_changed(self):
        self.selected_items = [checkbox.text() for list_item, checkbox in self.all_checkboxes if checkbox.isChecked()]
        self.update_selected_display()

    def select_all_symbols(self):
        for list_item, checkbox in self.all_checkboxes:
            if not list_item.isHidden():
                checkbox.setChecked(True)
        self.on_checkbox_changed()

    def clear_all_symbols(self):
        for list_item, checkbox in self.all_checkboxes:
            checkbox.setChecked(False)
        self.selected_items = []
        self.update_selected_display()

    def update_selected_display(self):
        self.selected_label.setText(", ".join(self.selected_items) if self.selected_items else "No symbols selected")
        self.selected_label.adjustSize()

    def get_selected_symbols(self):
        return self.selected_items

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Downloader")
        self.setMinimumSize(1200, 800)
        self.df_dict = {}
        self.symbol_cache = {}
        self.exchanges = ["NSE", "AMEX", "SNP 500"]
        self.selected_symbols = []
        self.init_ui()
        self.fetch_symbols("NSE")

    def init_ui(self):
        self.setStyleSheet("""
            QWidget {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #0f172a, stop:1 #1e293b);
                color: #e2e8f0;
                font-family: Inter;
            }
            QFrame#card {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 16px;
                padding: 24px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            QLabel {
                color: #e2e8f0;
                font-size: 16px;
                font-weight: 400;
            }
            QLabel#title {
                color: #f1f5f9;
                font-size: 40px;
                font-weight: 700;
                margin: 16px 0;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QLabel#section {
                color: #f1f5f9;
                font-size: 22px;
                font-weight: 600;
                margin-bottom: 12px;
            }
            QLineEdit, QDateEdit {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                padding: 12px 16px;
                color: #e2e8f0;
                font-size: 16px;
                selection-background-color: #2563eb;
                min-width: 150px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QLineEdit:focus, QDateEdit:focus {
                border-color: #60a5fa;
                box-shadow: 0 0 0 3px rgba(96, 165, 250, 0.2);
                outline: none;
            }
            QComboBox {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                padding: 12px 16px;
                color: #e2e8f0;
                font-size: 16px;
                min-width: 150px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QComboBox:focus {
                border-color: #60a5fa;
                box-shadow: 0 0 0 3px rgba(96, 165, 250, 0.2);
                outline: none;
            }
            QComboBox::drop-down {
                border: none;
                width: 32px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 6px solid #94a3b8;
                margin-right: 12px;
            }
            QComboBox QAbstractItemView {
                background: #1e293b;
                color: #e2e8f0;
                selection-background-color: #2563eb;
                border: 1px solid #475569;
                border-radius: 12px;
                padding: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QListWidget {
                background: #1e293b;
                border: 1px solid #475569;
                border-radius: 12px;
                color: #e2e8f0;
                font-size: 14px;
                padding: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QScrollBar:vertical {
                background: transparent;
                width: 12px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical {
                background: #64748b;
                border-radius: 6px;
                min-height: 20px;
                margin: 2px;
            }
            QScrollBar::handle:vertical:hover {
                background: #94a3b8;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QStatusBar {
                background: transparent;
                color: #94a3b8;
                border-top: 1px solid #475569;
                padding: 8px;
                font-size: 14px;
            }
            QProgressBar {
                border: none;
                border-radius: 6px;
                text-align: center;
                font-weight: 500;
                background: #334155;
                color: #e2e8f0;
                height: 12px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #3b82f6, stop:1 #2563eb);
                border-radius: 6px;
            }
            QMessageBox {
                background: #1e293b;
                color: #e2e8f0;
                border: 1px solid #475569;
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            QMessageBox QLabel {
                background: transparent;
                color: #e2e8f0;
                font-size: 16px;
            }
            QMessageBox QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3b82f6, stop:1 #2563eb);
                color: #ffffff;
                border: none;
                border-radius: 12px;
                padding: 10px 20px;
                font-weight: 600;
                min-width: 80px;
                font-size: 16px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QMessageBox QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #60a5fa, stop:1 #3b82f6);
            }
        """)

        main_layout = QVBoxLayout()
        main_layout.setSpacing(40)
        main_layout.setContentsMargins(60, 60, 60, 60)
        self.setLayout(main_layout)

        header_layout = QVBoxLayout()
        title_label = QLabel("Data Downloader")
        title_label.setObjectName("title")
        title_label.setAlignment(Qt.AlignCenter)
        header_layout.addWidget(title_label)
        main_layout.addLayout(header_layout)

        form_card = QFrame()
        form_card.setObjectName("card")
        form_layout = QVBoxLayout(form_card)
        form_layout.setSpacing(20)

        inputs_layout = QHBoxLayout()
        inputs_layout.setSpacing(32)

        symbol_layout = QVBoxLayout()
        symbol_label = QLabel("Selected Symbols (Edit Below)")
        symbol_label.setStyleSheet("font-size: 25px; font-weight: 500; color: #94a3b8; margin-bottom: 8px;")
        symbol_layout.addWidget(symbol_label)

        # Editable selected symbols input
        self.selected_symbols_input = QLineEdit()
        self.selected_symbols_input.setFixedHeight(50)
        self.selected_symbols_input.setPlaceholderText("Enter symbols (e.g., RELIANCE,INFY,TCS)")
        self.selected_symbols_input.setText(", ".join(self.selected_symbols) if self.selected_symbols else "")
        self.selected_symbols_input.editingFinished.connect(self.update_selected_symbols_from_input)
        symbol_layout.addWidget(self.selected_symbols_input)

        self.select_symbols_button = ModernButton("Select Symbols", primary=True)
        self.select_symbols_button.clicked.connect(self.open_symbol_selection_dialog)
        symbol_layout.addWidget(self.select_symbols_button)

        exchange_layout = QVBoxLayout()
        exchange_label = QLabel("Exchange")
        exchange_label.setStyleSheet("font-size: 25px; font-weight: 500; color: #94a3b8; margin-bottom: 8px;")
        exchange_layout.addWidget(exchange_label)
        self.exchange_combo = QComboBox()
        self.exchange_combo.addItems(self.exchanges)
        self.exchange_combo.setCurrentText("NSE")
        self.exchange_combo.setFixedHeight(50)
        self.exchange_combo.currentTextChanged.connect(self.on_exchange_changed)
        exchange_layout.addWidget(self.exchange_combo)

        interval_layout = QVBoxLayout()
        interval_label = QLabel("Interval")
        interval_label.setStyleSheet("font-size: 25px; font-weight: 500; color: #94a3b8; margin-bottom: 8px;")
        interval_layout.addWidget(interval_label)
        self.interval_input = QComboBox()
        self.interval_input.addItems(["1D", "1W", "1M"])
        self.interval_input.setCurrentText("1D")
        self.interval_input.setFixedHeight(50)
        interval_layout.addWidget(self.interval_input)

        dates_layout = QVBoxLayout()
        dates_label = QLabel("Date Range")
        dates_label.setStyleSheet("font-size: 25px; font-weight: 500; color: #94a3b8; margin-bottom: 8px;")
        dates_layout.addWidget(dates_label)
        date_inputs_layout = QHBoxLayout()
        self.from_date = QDateEdit()
        self.from_date.setFixedHeight(50)
        self.from_date.setCalendarPopup(True)
        self.from_date.setDate(QDate.currentDate().addYears(-1))
        date_inputs_layout.addWidget(self.from_date)
        self.to_date = QDateEdit()
        self.to_date.setFixedHeight(50)
        self.to_date.setCalendarPopup(True)
        self.to_date.setDate(QDate.currentDate())
        date_inputs_layout.addWidget(self.to_date)
        dates_layout.addLayout(date_inputs_layout)

        inputs_layout.addLayout(symbol_layout, 2)
        inputs_layout.addLayout(exchange_layout, 2)
        inputs_layout.addLayout(interval_layout, 1)
        inputs_layout.addLayout(dates_layout, 2)
        form_layout.addLayout(inputs_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(12)
        self.progress_bar.setVisible(False)
        form_layout.addWidget(self.progress_bar)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(16)
        button_layout.addStretch()
        self.fetch_button = ModernButton("Fetch Data", primary=True)
        self.export_button = ModernButton("Export CSV", primary=False)
        self.export_button.setEnabled(False)
        self.export_button.setToolTip("Fetch data first to enable export")
        button_layout.addWidget(self.export_button)
        button_layout.addWidget(self.fetch_button)
        form_layout.addLayout(button_layout)
        main_layout.addWidget(form_card)

        log_card = QFrame()
        log_card.setObjectName("card")
        log_layout = QVBoxLayout(log_card)
        log_header = QLabel("Fetch Logs")
        log_header.setObjectName("section")
        log_layout.addWidget(log_header)
        self.log_widget = QListWidget()
        self.log_widget.setStyleSheet("""
            QListWidget {
                background: #1e293b;
                border-radius: 12px;
                color: #e2e8f0;
                font-size: 14px;
                padding: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            QListWidget::item {
                padding: 6px;
            }
        """)
        self.log_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        log_layout.addWidget(self.log_widget)
        main_layout.addWidget(log_card, 1)

        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(40)
        self.status_bar.showMessage("Ready to fetch data")
        main_layout.addWidget(self.status_bar)

        self.fetch_button.clicked.connect(self.fetch_data)
        self.export_button.clicked.connect(self.export_csv)

    def update_selected_symbols_from_input(self):
        text = self.selected_symbols_input.text().strip()
        if not text:
            self.selected_symbols = []
            self.add_log("Cleared all selected symbols", "#f59e0b")
        else:
            symbols = [s.strip().upper() for s in text.split(",") if s.strip()]
            self.selected_symbols = sorted(list(set(symbols)))  # Remove duplicates
            self.add_log(f"Updated selected symbols: {', '.join(self.selected_symbols)}", "#60a5fa")
        self.selected_symbols_input.setText(", ".join(self.selected_symbols))

    def open_symbol_selection_dialog(self):
        exchange = self.exchange_combo.currentText()
        symbols = self.symbol_cache.get(exchange, [])
        if not symbols:
            self.show_error(f"No symbols available for {exchange}. Please try a different exchange.")
            return

        dialog = SymbolSelectionDialog(self, symbols, self.selected_symbols)
        if dialog.exec():
            self.selected_symbols = dialog.get_selected_symbols()
            self.selected_symbols_input.setText(", ".join(self.selected_symbols))
            self.add_log(f"Selected {len(self.selected_symbols)} symbols: {', '.join(self.selected_symbols)}", "#22c55e")

    def fetch_symbols(self, exchange):
        self.exchange_combo.setEnabled(False)
        self.select_symbols_button.setEnabled(False)
        self.status_bar.showMessage(f"Fetching symbols for {exchange}")
        self.add_log(f"Fetching symbols for {exchange}", "#60a5fa")
        self.symbol_thread = SymbolFetchThread(exchange)
        self.symbol_thread.symbols_fetched.connect(self.on_symbols_fetched)
        self.symbol_thread.error.connect(self.show_error)
        self.symbol_thread.finished.connect(self.symbol_thread.deleteLater)
        self.symbol_thread.start()

    def on_symbols_fetched(self, symbols):
        exchange = self.exchange_combo.currentText()
        self.exchange_combo.setEnabled(True)
        self.select_symbols_button.setEnabled(True)
        if not symbols:
            self.show_error(f"No symbols found for {exchange}")
            self.status_bar.showMessage(f"No symbols found for {exchange}")
            return
        self.symbol_cache[exchange] = symbols
        # Only keep selected symbols that are valid for the new exchange, unless manually added
        fetched_symbols = set(symbols)
        self.selected_symbols = [s for s in self.selected_symbols if s in fetched_symbols]
        self.selected_symbols_input.setText(", ".join(self.selected_symbols))
        self.add_log(f"Loaded {len(symbols)} symbols for {exchange}", "#22c55e")
        self.status_bar.showMessage(f"Loaded {len(symbols)} symbols for {exchange}")

    def on_exchange_changed(self, exchange):
        if exchange in self.exchanges:
            self.fetch_symbols(exchange)

    def add_log(self, message, color="#e2e8f0"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item = QListWidgetItem(f"[{timestamp}] {message}")
        item.setForeground(QColor(color))
        self.log_widget.addItem(item)
        self.log_widget.scrollToBottom()

    def fetch_data(self):
        # Use the symbols from the editable input
        self.update_selected_symbols_from_input()  # Ensure the list is up-to-date
        symbols = self.selected_symbols.copy()

        if not symbols:
            self.add_log("No symbols provided", "#f59e0b")
            self.show_error("Please select or enter at least one symbol to fetch data")
            return

        exchange = self.exchange_combo.currentText().strip().upper() or "NSE"
        interval_str = self.interval_input.currentText()
        from_date = self.from_date.date().toPython()
        to_date = self.to_date.date().toPython()

        if from_date > to_date:
            self.show_error("From date must be earlier than To date")
            return

        interval_map = {"1D": Interval.in_daily, "1W": Interval.in_weekly, "1M": Interval.in_monthly}
        interval = interval_map.get(interval_str, Interval.in_daily)

        delta = to_date - from_date
        n_bars = delta.days + 1
        if n_bars <= 0:
            self.show_error("Invalid date range")
            return

        # Log symbols not in the fetched list
        fetched_symbols = set(self.symbol_cache.get(exchange, []))
        manual_symbols = [s for s in symbols if s not in fetched_symbols]
        if manual_symbols:
            self.add_log(f"Note: These symbols may be invalid for {exchange}: {', '.join(manual_symbols)}", "#f59e0b")

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.fetch_button.setEnabled(False)
        self.export_button.setEnabled(False)
        self.select_symbols_button.setEnabled(False)
        self.exchange_combo.setEnabled(False)
        self.fetch_button.setText("Fetching...")
        self.status_bar.showMessage(f"Fetching data for {len(symbols)} symbols")
        self.add_log(f"Starting fetch for {len(symbols)} symbols from {exchange}", "#60a5fa")

        try:
            tv = TvDatafeed()
            self.fetch_thread = DataFetchThread(
                tv,
                symbols,
                exchange,
                interval,
                n_bars,
            )
            self.fetch_thread.data_fetched.connect(self.on_data_fetched)
            self.fetch_thread.error.connect(self.show_error)
            self.fetch_thread.progress.connect(self.progress_bar.setValue)
            self.fetch_thread.log_message.connect(self.add_log)
            self.fetch_thread.finished.connect(self.reset_fetch_button)
            self.fetch_thread.start()
        except Exception as e:
            self.show_error(f"Failed to initialize fetch: {str(e)}")
            self.reset_fetch_button()

    def on_data_fetched(self, df_dict):
        self.df_dict = df_dict
        total_records = sum(len(df) for df in df_dict.values())
        selected_symbols = self.selected_symbols

        if not df_dict:
            self.show_error("No data returned. Check symbols or connection.")
            self.export_button.setEnabled(False)
            self.export_button.setToolTip("No data available to export")
            self.status_bar.showMessage("No data returned")
            self.add_log("Fetch failed: No data returned", "#ef4444")
            return

        self.export_button.setEnabled(True)
        self.export_button.setToolTip("Export fetched data to CSV")
        if len(df_dict) < len(selected_symbols):
            failed_symbols = [s for s in selected_symbols if s not in df_dict]
            self.show_error(f"Failed to fetch data for {', '.join(failed_symbols)}")
            self.add_log(f"Fetch incomplete: {len(df_dict)}/{len(selected_symbols)} symbols", "#ef4444")
            self.status_bar.showMessage(f"Fetched {total_records} records for {len(df_dict)} symbols")
        else:
            self.show_info("Data Fetched Successfully", f"Fetched {total_records} records for {len(df_dict)} symbols")
            self.add_log(f"Fetch complete: {total_records} records for {len(df_dict)} symbols", "#22c55e")
            self.status_bar.showMessage(f"Fetched {total_records} records for {len(df_dict)} symbols")

    def reset_fetch_button(self):
        self.progress_bar.setVisible(False)
        self.fetch_button.setEnabled(True)
        self.fetch_button.setText("Fetch Data")
        self.select_symbols_button.setEnabled(True)
        self.exchange_combo.setEnabled(True)
        if self.df_dict:
            total_records = sum(len(df) for df in self.df_dict.values())
            self.status_bar.showMessage(f"Fetched {total_records} records • Ready for export")
            self.export_button.setEnabled(True)
            self.export_button.setToolTip("Export fetched data to CSV")
            self.add_log(f"Export button enabled: {len(self.df_dict)} symbols available", "#60a5fa")
        else:
            self.status_bar.showMessage("Ready to fetch data")
            self.export_button.setEnabled(False)
            self.export_button.setToolTip("Fetch data first to enable export")

    def export_csv(self):
        if not self.df_dict:
            self.show_error("No data to export")
            return

        base_dir = os.path.expanduser("~/download/data")
        exchange = self.exchange_combo.currentText().strip().upper()
        exchange_dir = os.path.join(base_dir, exchange)
        try:
            os.makedirs(exchange_dir, exist_ok=True)
        except Exception as e:
            self.show_error(f"Failed to create directory {exchange_dir}: {str(e)}")
            return

        exported_files = []
        total_rows = 0
        for symbol, df in self.df_dict.items():
            try:
                df_cleaned = df.copy()
                date_columns = ["datetime", "candle_timestamp"]
                date_col = next((col for col in date_columns if col in df_cleaned.columns), None)
                if date_col:
                    try:
                        df_cleaned[date_col] = pd.to_datetime(df_cleaned[date_col]).dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        self.add_log(f"Date conversion failed for {symbol}: {str(e)}", "#f59e0b")

                if "symbol" in df_cleaned.columns:
                    df_cleaned["symbol"] = df_cleaned["symbol"].str.replace(f"{exchange}:", "", regex=False)

                filename = os.path.join(exchange_dir, f"{symbol}.csv")
                df_cleaned.to_csv(filename, index=False)
                total_rows += len(df_cleaned)
                exported_files.append(os.path.basename(filename))
                self.add_log(f"Exported {len(df_cleaned)} rows for {symbol}", "#22c55e")
            except Exception as e:
                self.show_error(f"Failed to export {symbol}: {str(e)}")
                return

        success_message = (
            f"Exported {len(exported_files)} files\n"
            f"Total rows: {total_rows}\n"
            f"Location: {exchange_dir}"
        )
        self.show_info("Export Complete", success_message)
        self.status_bar.showMessage(f"Exported {len(exported_files)} files with {total_rows} rows")
        try:
            import subprocess
            if sys.platform.startswith("win"):
                os.startfile(exchange_dir)
            elif sys.platform.startswith("linux"):
                subprocess.Popen(["xdg-open", exchange_dir])
            elif sys.platform.startswith("darwin"):
                subprocess.Popen(["open", exchange_dir])
        except:
            pass

    def show_error(self, message):
        self.add_log(message, "#ef4444")
        self.status_bar.showMessage(message)
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("Error")
        msg.setText(message)
        msg.setStyleSheet(self.styleSheet())
        msg.exec()

    def show_info(self, title, message):
        self.add_log(message, "#22c55e")
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle(title)
        msg.setText(message)
        msg.setStyleSheet(self.styleSheet())
        msg.exec()

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("TradingView Data")
    app.setApplicationVersion("2.2")

    splash = LoadingScreen()
    splash.show()
    app.processEvents()
    time.sleep(1.5)

    window = MainWindow()
    splash.close()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()