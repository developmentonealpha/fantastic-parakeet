from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QFileDialog, QFrame, QMessageBox, 
    QProgressBar, QSplashScreen, QStatusBar, QListWidget, QListWidgetItem, QCheckBox, QDateEdit
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QDate
from PySide6.QtGui import QFont, QPalette, QColor, QLinearGradient, QBrush, QPixmap, QPainter, QIcon
import sys
import pandas as pd
import os
from datetime import datetime
import time
import requests

try:
    from main_backup import TvDatafeed, Interval
except ImportError:
    print("Error: Could not import TvDatafeed and Interval from main.py")
    print("Please ensure main.py is in the same directory")
    sys.exit(1)

# Default symbol lists as fallback
DEFAULT_SYMBOLS_NSE = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR",
    "ICICIBANK", "KOTAKBANK", "SBIN", "BAJFINANCE", "BHARTIARTL",
    "ASIANPAINT", "ITC", "HCLTECH", "AXISBANK", "MARUTI",
    "SUNPHARMA", "WIPRO", "ULTRACEMCO", "TITAN", "ADANIENT"
]

DEFAULT_SYMBOLS_SNP500 = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META",
    "TSLA", "NVDA", "JPM", "V", "WMT",
    "JNJ", "PG", "UNH", "HD", "DIS",
    "PYPL", "NFLX", "ADBE", "CMCSA", "PEP"
]

ETF_STOCKS = [
    'SPY', 'IWM', 'MDY', 'QQQ', 'VTV',
    'VUG', 'RSP', 'DIA', 'XLF', 'XLK',
    'XLE', 'XLV', 'XLI', 'XLY', 'XLP',
    'XLU', 'XLB', 'XLRE', 'EWJ', 'EWG',
    'EWZ', 'EWC', 'EWA', 'EWT', 'EWY',
    'EWH', 'EWS', 'EWM', 'TLT', 'IEF',
    'SHY', 'LQD', 'HYG', 'TIP', 'EMB',
    'BNDX', 'GLD', 'SLV', 'USO', 'UNG',
    'DBA', 'DBB', 'UUP', 'FXE', 'FXY',
    'FXB', 'VNQ', 'RWX', 'PFF', 'VIG'
]

# Mapping of exchanges to TradingView regions and markets (for NSE and SNP 500)
EXCHANGE_MAP = {
    "NSE": {"region": "india", "market": "nse"},
    "AMEX": {"region": "america", "market": "amex"},
    "SNP 500": {"region": "america", "markets": ["nyse", "nasdaq"]}
}

def get_symbols(exchange):
    try:
        # Directly return ETF_STOCKS for ETF exchange
        if exchange == "exchange":
            return sorted(ETF_STOCKS)

        if exchange not in EXCHANGE_MAP:
            raise ValueError(f"Exchange {exchange} not supported by TradingView API")

        # Handle S&P 500 separately since it spans multiple markets
        if exchange == "SNP 500":
            symbols = set()
            for market in EXCHANGE_MAP[exchange]["markets"]:
                url = f"https://scanner.tradingview.com/{EXCHANGE_MAP[exchange]['region']}/scan"
                payload = {
                    "filter": [],
                    "options": {"lang": "en"},
                    "markets": [market],
                    "symbols": {"query": {"types": []}, "tickers": []},
                    "columns": ["name"]
                }
                headers = {
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                market_symbols = {item['d'][0] for item in data.get('data', []) if item['d'][0]}
                symbols.update(market_symbols)
            symbols = sorted(list(symbols))
            return symbols if symbols else DEFAULT_SYMBOLS_SNP500
        else:
            # Handle NSE
            region = EXCHANGE_MAP[exchange]["region"]
            market = EXCHANGE_MAP[exchange]["market"]
            url = f"https://scanner.tradingview.com/{region}/scan"
            payload = {
                "filter": [],
                "options": {"lang": "en"},
                "markets": [market],
                "symbols": {"query": {"types": []}, "tickers": []},
                "columns": ["name"]
            }
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            symbols = sorted(list(set(item['d'][0] for item in data.get('data', []) if item['d'][0])))
            return symbols if symbols else DEFAULT_SYMBOLS_NSE
    except Exception as e:
        print(f"Error fetching symbols for {exchange}: {str(e)}")
        if exchange == "SNP 500":
            return DEFAULT_SYMBOLS_SNP500
        elif exchange == "AMEX":
            return ETF_STOCKS
        else:
            return DEFAULT_SYMBOLS_NSE

class SymbolFetchThread(QThread):
    symbols_fetched = Signal(list)
    error = Signal(str)

    def __init__(self, exchange):
        super().__init__()
        self.exchange = exchange

    def run(self):
        try:
            symbols = get_symbols(self.exchange)
            if symbols:
                self.symbols_fetched.emit(symbols)
            else:
                self.error.emit(f"No symbols returned for {self.exchange}")
        except Exception as e:
            self.error.emit(f"Failed to fetch symbols for {self.exchange}: {str(e)}")

class LoadingScreen(QSplashScreen):
    def __init__(self):
        pixmap = QPixmap(400, 300)
        pixmap.fill(Qt.transparent)
        
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        
        gradient = QLinearGradient(0, 0, 0, 300)
        gradient.setColorAt(0, QColor("#1c2526"))
        gradient.setColorAt(1, QColor("#1c1c1e"))
        painter.fillRect(pixmap.rect(), gradient)
        painter.setPen(QColor("#ffffff"))
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
    log_message = Signal(str, str)  # Message, color

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
            processed_symbols = 0

            for symbol in self.symbols:
                success = False
                for attempt in range(self.max_retries):
                    self.log_message.emit(
                        f"Fetching {self.n_bars} rows for {symbol} (Attempt {attempt + 1}/{self.max_retries})",
                        "#60a5fa"
                    )
                    try:
                        df = self.tv.get_hist(
                            symbol=symbol,
                            exchange=self.exchange,
                            interval=self.interval,
                            n_bars=self.n_bars
                        )
                        if df is not None and not df.empty:
                            df = df.reset_index()
                            if 'timestamp' in df.columns:
                                df = df.rename(columns={'timestamp': 'datetime'})
                            if 'symbol' in df.columns:
                                df['symbol'] = df['symbol'].str.replace(f"{self.exchange}:", "", regex=False)
                            results[symbol] = df
                            self.log_message.emit(
                                f"Downloaded {len(df)} rows for {symbol}",
                                "#22c55e"
                            )
                            success = True
                            break
                        else:
                            self.log_message.emit(
                                f"No data returned for {symbol} (Attempt {attempt + 1})",
                                "#ef4444"
                            )
                    except Exception as e:
                        self.log_message.emit(
                            f"Error fetching {symbol} (Attempt {attempt + 1}): {str(e)}",
                            "#ef4444"
                        )
                    time.sleep(2)

                if not success:
                    failed_symbols.append(symbol)
                    self.log_message.emit(
                        f"Failed to download data for {symbol} after {self.max_retries} attempts",
                        "#ef4444"
                    )

                processed_symbols += 1
                self.progress.emit(int((processed_symbols / total_symbols) * 100))

            retry_round = 1
            while failed_symbols:
                self.log_message.emit(
                    f"Retry round {retry_round} for {len(failed_symbols)} failed symbols: {', '.join(failed_symbols)}",
                    "#f59e0b"
                )
                new_failed = []
                for symbol in failed_symbols:
                    success = False
                    for attempt in range(self.max_retries):
                        self.log_message.emit(
                            f"Retrying {symbol} (Round {retry_round}, Attempt {attempt + 1}/{self.max_retries})",
                            "#60a5fa"
                        )
                        try:
                            df = self.tv.get_hist(
                                symbol=symbol,
                                exchange=self.exchange,
                                interval=self.interval,
                                n_bars=self.n_bars
                            )
                            if df is not None and not df.empty:
                                df = df.reset_index()
                                if 'timestamp' in df.columns:
                                    df = df.rename(columns={'timestamp': 'datetime'})
                                if 'symbol' in df.columns:
                                    df['symbol'] = df['symbol'].str.replace(f"{self.exchange}:", "", regex=False)
                                results[symbol] = df
                                self.log_message.emit(
                                    f"Downloaded {len(df)} rows for {symbol} on retry",
                                    "#22c55e"
                                )
                                success = True
                                break
                            else:
                                self.log_message.emit(
                                    f"No data returned for {symbol} (Retry attempt {attempt + 1})",
                                    "#ef4444"
                                )
                        except Exception as e:
                            self.log_message.emit(
                                f"Error retrying {symbol} (Attempt {attempt + 1}): {str(e)}",
                                "#ef4444"
                            )
                        time.sleep(2)

                    if not success:
                        new_failed.append(symbol)

                failed_symbols = new_failed
                retry_round += 1
                if retry_round > self.max_retries:
                    break

            if failed_symbols:
                self.log_message.emit(
                    f"Final failures after all retries: {', '.join(failed_symbols)}",
                    "#ef4444"
                )
            else:
                self.log_message.emit(
                    f"All {total_symbols} symbols fetched successfully",
                    "#22c55e"
                )

            self.data_fetched.emit(results)
        except Exception as e:
            self.error.emit(str(e))

class ModernButton(QPushButton):
    def __init__(self, text, primary=True):
        super().__init__(text)
        self.primary = primary
        self.setFixedHeight(48)
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
                    background-color: #2563eb;
                    color: white;
                    border: none;
                    border-radius: 10px;
                    font-size: 16px;
                    font-weight: 600;
                    font-family: Inter;
                    padding: 12px 24px;
                    icon-size: 20px;
                }
                QPushButton:hover {
                    background-color: #1d4ed8;
                }
                QPushButton:pressed {
                    background-color: #1e40af;
                }
                QPushButton:disabled {
                    background-color: #4b5563;
                    color: #9ca3af;
                }
            """)
        else:
            self.setStyleSheet("""
                QPushButton {
                    background-color: #374151;
                    color: #60a5fa;
                    border: 1px solid #4b5563;
                    border-radius: 10px;
                    font-size: 16px;
                    font-weight: 600;
                    font-family: Inter;
                    padding: 12px 24px;
                    icon-size: 20px;
                }
                QPushButton:hover {
                    background-color: #4b5563;
                    border-color: #6b7280;
                }
                QPushButton:pressed {
                    background-color: #6b7280;
                }
                QPushButton:disabled {
                    background-color: #374151;
                    color: #9ca3af;
                    border-color: #4b5563;
                }
            """)

class SymbolSelector(QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window  # Store reference to MainWindow
        self.items = []
        self.selected_items = []
        
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        
        # Label for the section
        label = QLabel("Select Symbols")
        label.setStyleSheet("font-size: 15px; font-weight: 300; color: #a1a1aa; margin-bottom: 8px;")
        layout.addWidget(label)
        
        # Display selected symbols
        self.selected_label = QLabel("No symbols selected")
        self.selected_label.setStyleSheet("""
            QLabel {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 16px;
                font-family: Inter;
                min-height: 48px;
            }
        """)
        self.selected_label.setWordWrap(True)
        layout.addWidget(self.selected_label)
        
        # Search input
        self.search_input = QLineEdit()
        self.search_input.setFixedHeight(48)
        self.search_input.setFixedWidth(450)
        self.search_input.setPlaceholderText("Type to search symbols...")
        self.search_input.setStyleSheet("""
            QLineEdit {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 16px;
                font-family: Inter;
            }
            QLineEdit:focus {
                border-color: #60a5fa;
                outline: none;
            }
        """)
        layout.addWidget(self.search_input)
        
        # Dropdown (QListWidget)
        self.list_widget = QListWidget()
        self.list_widget.setStyleSheet("""
            QListWidget {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                color: #ffffff;
                font-size: 15px;
                padding: 8px;
            }
            QListWidget::item {
                background-color: transparent;
                padding: 5px 8px;
                border-bottom: 1px solid #4b5563;
            }
        """)
        self.list_widget.setMinimumWidth(400)
        self.list_widget.setMaximumHeight(300)
        self.list_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.list_widget.setFocusPolicy(Qt.NoFocus)
        self.list_widget.setHidden(True)  # Initially hidden
        layout.addWidget(self.list_widget)
        
        # Buttons (Select All, Clear All)
        button_layout = QHBoxLayout()
        self.select_all_button = ModernButton("Select All", primary=False)
        self.select_all_button.clicked.connect(self.select_all_symbols)
        self.clear_all_button = ModernButton("Clear All", primary=False)
        self.clear_all_button.clicked.connect(self.clear_all_symbols)
        button_layout.addWidget(self.select_all_button)
        button_layout.addWidget(self.clear_all_button)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        self.all_checkboxes = []
        
        # Connect search input to filtering
        self.search_input.textChanged.connect(self.on_search_text_changed)
        # Connect focus events to show/hide dropdown
        self.search_input.editingFinished.connect(self.hide_dropdown)
        # Add focus in/out events to manage dropdown visibility
        self.search_input.focusInEvent = lambda event: self.show_dropdown()
        self.search_input.focusOutEvent = lambda event: self.hide_dropdown()

    def show_dropdown(self):
        # Show the dropdown when the search input gains focus
        if self.list_widget.count() > 0:
            self.list_widget.setHidden(False)

    def on_search_text_changed(self, text):
        # Filter symbols based on the input text
        self.filter_symbols(text)
        # Show the dropdown if there are items to display
        if self.list_widget.count() > 0 and any(not item.isHidden() for item in [self.list_widget.item(i) for i in range(self.list_widget.count())]):
            self.list_widget.setHidden(False)
        else:
            self.list_widget.setHidden(True)
        # Debug log to track selected items during search
        self.main_window.add_log(f"Search text changed to '{text}', selected items: {self.selected_items}", "#60a5fa")

    def hide_dropdown(self):
        # Hide the dropdown when the search input loses focus
        if not self.search_input.hasFocus():
            self.list_widget.setHidden(True)

    def update_symbols(self, symbols):
        self.items = sorted(symbols)  # Already sorted in get_symbols, but ensure here as well
        self.list_widget.clear()
        self.all_checkboxes = []
        self.selected_items = []
        
        for item in self.items:
            list_item = QListWidgetItem()
            checkbox = QCheckBox(item)
            checkbox.setStyleSheet("""
                QCheckBox {
                    background-color: transparent;
                    color: #ffffff;
                    font-size: 15px;
                    border: none;
                    spacing: 12px;
                }
                QCheckBox::indicator {
                    width: 18px;
                    height: 18px;
                    background-color: #374151;
                    border: 1px solid #4b5563;
                    border-radius: 4px;
                }
                QCheckBox::indicator:checked {
                    background-color: #2563eb;
                    border-color: #2563eb;
                }
            """)
            checkbox.stateChanged.connect(self.on_checkbox_changed)
            self.list_widget.addItem(list_item)
            self.list_widget.setItemWidget(list_item, checkbox)
            self.all_checkboxes.append((list_item, checkbox))
        
        self.update_selected_display()
        self.main_window.add_log(f"Updated symbols: {len(symbols)} symbols loaded", "#22c55e")

    def filter_symbols(self, text):
        text = text.lower().strip()
        for list_item, checkbox in self.all_checkboxes:
            symbol = checkbox.text().lower()
            list_item.setHidden(text != "" and text not in symbol)

    def on_checkbox_changed(self):
        self.selected_items = []
        # Include all checked symbols, regardless of visibility
        for list_item, checkbox in self.all_checkboxes:
            if checkbox.isChecked():
                self.selected_items.append(checkbox.text())
        self.update_selected_display()
        # Debug log to track selections
        self.main_window.add_log(f"Checkbox changed, selected items: {self.selected_items}", "#22c55e")

    def select_all_symbols(self):
        for list_item, checkbox in self.all_checkboxes:
            if not list_item.isHidden():
                checkbox.setChecked(True)
        self.on_checkbox_changed()
        self.main_window.add_log("Select All clicked", "#60a5fa")

    def clear_all_symbols(self):
        for list_item, checkbox in self.all_checkboxes:
            checkbox.setChecked(False)
        self.selected_items = []
        self.update_selected_display()
        self.main_window.add_log("Clear All clicked", "#60a5fa")

    def update_selected_display(self):
        if self.selected_items:
            self.selected_label.setText(", ".join(self.selected_items))
        else:
            self.selected_label.setText("No symbols selected")
        # Adjust the height of the label based on content
        self.selected_label.adjustSize()

    def get_selected_symbols(self):
        return self.selected_items

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Downloader")
        self.setMinimumSize(1200, 800)
        self.df_dict = {}
        self.symbol_cache = {}  # Cache for exchange symbols
        
        self.setStyleSheet("""
            QWidget {
                background-color: #1c2526;
                color: #ffffff;
                font-family: Inter, system-ui;
            }
            
            QFrame#card {
                background-color: #1c2526;
                border: 1px solid #4b5563;
                border-radius: 12px;
                padding: 24px;
            }
            
            QLabel {
                color: #ffffff;
                font-size: 16px;
                font-weight: 400;
                background-color: transparent;
            }
            
            QLabel#title {
                color: #ffffff;
                font-size: 36px;
                font-weight: 700;
                margin: 16px 0;
                background-color: transparent;
            }
            
            QLabel#section {
                color: #ffffff;
                font-size: 20px;
                font-weight: 600;
                margin-bottom: 12px;
                background-color: transparent;
            }
            
            QLineEdit, QDateEdit {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 16px;
                font-family: Inter;
                selection-background-color: #2563eb;
                selection-color: white;
                min-width: 150px;
            }
            QLineEdit:focus, QDateEdit:focus {
                border-color: #60a5fa;
                outline: none;
            }
            QLineEdit::placeholder, QDateEdit::placeholder {
                color: #9ca3af;
            }
            
            QComboBox {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 16px;
                font-family: Inter;
                min-width: 150px;
            }
            QComboBox:focus {
                border-color: #60a5fa;
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
                border-top: 6px solid #9ca3af;
                margin-right: 12px;
            }
            QComboBox QAbstractItemView {
                background-color: #374151;
                color: #ffffff;
                selection-background-color: #2563eb;
                selection-color: white;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 8px;
                outline: none;
            }
            
            QListWidget {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 12px;
                color: #ffffff;
                font-size: 14px;
                padding: 8px;
            }
            QListWidget::item {
                padding: 4px;
            }
            
            QScrollBar:vertical {
                background-color: transparent;
                width: 12px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical {
                background-color: #9ca3af;
                border-radius: 6px;
                min-height: 20px;
                margin: 2px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #d1d5db;
            }
            
            QStatusBar {
                background-color: transparent;
                color: #a1a1aa;
                border-top: 1px solid #4b5563;
                padding: 8px;
                font-size: 13px;
                font-weight: 400;
            }
            
            QProgressBar {
                border: none;
                border-radius: 6px;
                text-align: center;
                font-weight: 500;
                background-color: #4b5563;
                color: #ffffff;
                height: 10px;
            }
            QProgressBar::chunk {
                background-color: #2563eb;
                border-radius: 6px;
            }
            
            QMessageBox {
                background-color: #374151;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #2563eb;
                color: white;
                border: none;
                border-radius: 10px;
                padding: 10px 20px;
                font-weight: 600;
                min-width: 80px;
                font-size: 16px;
            }
            QMessageBox QPushButton:hover {
                background-color: #1d4ed8;
            }
        """)

        self.exchanges = ["NSE", "AMEX", "SNP 500"]
        self.init_ui()
        self.fetch_symbols("NSE")  # Fetch NSE symbols on startup

    def init_ui(self):
        main_layout = QVBoxLayout()
        main_layout.setSpacing(32)
        main_layout.setContentsMargins(48, 48, 48, 48)
        self.setLayout(main_layout)

        header_layout = QVBoxLayout()
        
        title_label = QLabel("Data Downloader")
        title_label.setObjectName("title")
        title_label.setAlignment(Qt.AlignCenter)
        header_layout.addWidget(title_label)
        main_layout.addLayout(header_layout)

        form_card = QFrame()
        form_card.setObjectName("card")
        form_layout = QVBoxLayout()
        form_layout.setSpacing(16)
        form_card.setLayout(form_layout)

        inputs_layout = QHBoxLayout()
        inputs_layout.setSpacing(24)
        
        symbol_layout = QVBoxLayout()
        self.symbol_input = SymbolSelector(self)  # Pass self as main_window to access add_log
        symbol_layout.addWidget(self.symbol_input)
        
        exchange_layout = QVBoxLayout()
        exchange_label = QLabel("Exchange")
        exchange_label.setStyleSheet("font-size: 13px; font-weight: 500; color: #a1a1aa; margin-bottom: 8px;")
        exchange_layout.addWidget(exchange_label)
        self.exchange_combo = QComboBox()
        self.exchange_combo.addItems(self.exchanges)
        self.exchange_combo.setCurrentText("NSE")
        self.exchange_combo.setFixedHeight(48)
        self.exchange_combo.setEditable(True)
        self.exchange_combo.currentTextChanged.connect(self.on_exchange_changed)
        exchange_layout.addWidget(self.exchange_combo)
        
        interval_layout = QVBoxLayout()
        interval_label = QLabel("Interval")
        interval_label.setStyleSheet("font-size: 13px; font-weight: 500; color: #a1a1aa; margin-bottom: 8px;")
        interval_layout.addWidget(interval_label)
        self.interval_input = QComboBox()
        self.interval_input.addItems(["1D", "1W", "1M"])
        self.interval_input.setCurrentText("1D")
        self.interval_input.setFixedHeight(48)
        interval_layout.addWidget(self.interval_input)
        
        dates_layout = QVBoxLayout()
        dates_label = QLabel("Date Range")
        dates_label.setStyleSheet("font-size: 13px; font-weight: 500; color: #a1a1aa; margin-bottom: 8px;")
        dates_layout.addWidget(dates_label)
        date_inputs_layout = QHBoxLayout()
        
        self.from_date = QDateEdit()
        self.from_date.setFixedHeight(48)
        self.from_date.setCalendarPopup(True)
        self.from_date.setDate(QDate.currentDate().addYears(-1))
        date_inputs_layout.addWidget(self.from_date)
        
        self.to_date = QDateEdit()
        self.to_date.setFixedHeight(48)
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
        self.progress_bar.setFixedHeight(10)
        self.progress_bar.setVisible(False)
        form_layout.addWidget(self.progress_bar)
        
        button_layout = QHBoxLayout()
        button_layout.setSpacing(12)
        button_layout.addStretch()
        
        self.fetch_button = ModernButton("Fetch Data", primary=True)
        self.export_button = ModernButton("Export CSV", primary=False)
        self.export_button.setEnabled(False)
        
        button_layout.addWidget(self.export_button)
        button_layout.addWidget(self.fetch_button)
        
        form_layout.addLayout(button_layout)
        main_layout.addWidget(form_card)

        log_card = QFrame()
        log_card.setObjectName("card")
        log_layout = QVBoxLayout()
        log_card.setLayout(log_layout)

        log_header = QLabel("Fetch Logs")
        log_header.setObjectName("section")
        log_layout.addWidget(log_header)

        self.log_widget = QListWidget()
        self.log_widget.setStyleSheet("""
            QListWidget {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 12px;
                color: #ffffff;
                font-size: 14px;
                padding: 8px;
            }
            QListWidget::item {
                padding: 4px;
            }
        """)
        self.log_widget.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        log_layout.addWidget(self.log_widget)
        
        main_layout.addWidget(log_card, 1)

        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(36)
        self.status_bar.showMessage("Ready to fetch data")
        main_layout.addWidget(self.status_bar)

        self.fetch_button.clicked.connect(self.fetch_data)
        self.export_button.clicked.connect(self.export_csv)

    def fetch_symbols(self, exchange):
        if exchange in self.symbol_cache:
            self.symbol_input.update_symbols(self.symbol_cache[exchange])
            self.add_log(f"Loaded {len(self.symbol_cache[exchange])} symbols for {exchange} from cache", "#22c55e")
            self.status_bar.showMessage(f"Loaded {len(self.symbol_cache[exchange])} symbols for {exchange}")
            return

        self.status_bar.showMessage(f"Fetching symbols for {exchange}...")
        self.add_log(f"Fetching symbols for {exchange}", "#60a5fa")
        self.symbol_thread = SymbolFetchThread(exchange)
        self.symbol_thread.symbols_fetched.connect(self.on_symbols_fetched)
        self.symbol_thread.error.connect(self.show_error)
        self.symbol_thread.finished.connect(self.symbol_thread.deleteLater)
        self.symbol_thread.start()

    def on_symbols_fetched(self, symbols):
        self.symbol_cache[self.exchange_combo.currentText()] = symbols
        self.symbol_input.update_symbols(symbols)
        self.add_log(f"Loaded {len(symbols)} symbols for {self.exchange_combo.currentText()}", "#22c55e")
        self.status_bar.showMessage(f"Loaded {len(symbols)} symbols for {self.exchange_combo.currentText()}")

    def on_exchange_changed(self, exchange):
        if exchange:
            self.fetch_symbols(exchange)

    def add_log(self, message, color="#ffffff"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item = QListWidgetItem(f"[{timestamp}] {message}")
        item.setForeground(QColor(color))
        self.log_widget.addItem(item)
        self.log_widget.scrollToBottom()

    def fetch_data(self):
        symbols = self.symbol_input.get_selected_symbols()
        if not symbols:
            self.show_error("Please select at least one symbol.")
            return
                
        exchange = self.exchange_combo.currentText().strip().upper() or "NSE"
        interval_str = self.interval_input.currentText()
        
        from_date = self.from_date.date().toPython()
        to_date = self.to_date.date().toPython()
        
        if from_date > to_date:
            self.show_error("From date must be earlier than To date.")
            return

        interval_map = {i.value: i for i in Interval}
        interval_enum = interval_map.get(interval_str, Interval.in_daily)

        delta = to_date - from_date
        n_bars = delta.days + 1

        if n_bars <= 0:
            self.show_error("Please select a valid date range.")
            return

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.fetch_button.setEnabled(False)
        self.fetch_button.setText("Fetching...")
        self.status_bar.showMessage(f"Fetching data for {', '.join(symbols)}")
        self.add_log(f"Starting fetch for {len(symbols)} symbols from {exchange}", "#60a5fa")

        try:
            tv = TvDatafeed()
            self.df_dict = {}
            self.thread = DataFetchThread(tv, symbols, exchange, interval_enum, n_bars)
            self.thread.data_fetched.connect(self.on_data_fetched)
            self.thread.error.connect(self.show_error)
            self.thread.progress.connect(self.progress_bar.setValue)
            self.thread.log_message.connect(self.add_log)
            self.thread.finished.connect(self.reset_fetch_button)
            self.thread.start()
        except Exception as e:
            self.show_error(f"Failed to initialize data fetcher: {str(e)}")
            self.reset_fetch_button()

    def on_data_fetched(self, df_dict):
        self.df_dict = df_dict
        total_records = sum(len(df) for df in df_dict.values())
        selected_symbols = self.symbol_input.get_selected_symbols()
        
        if not df_dict:
            self.show_error("No data returned. Please check symbols, exchange, or connection.")
            self.export_button.setEnabled(False)
            return

        if len(df_dict) < len(selected_symbols):
            failed_symbols = [s for s in selected_symbols if s not in df_dict]
            self.show_error(f"Failed to fetch data for {', '.join(failed_symbols)} after retries.")
            self.export_button.setEnabled(False)
            self.add_log(f"Fetch incomplete: {len(df_dict)}/{len(selected_symbols)} symbols successful", "#ef4444")
        else:
            self.show_info("Data Fetched Successfully", 
                          f"Successfully fetched {total_records} records for {len(df_dict)} symbols")
            self.export_button.setEnabled(True)
            self.add_log(f"Fetch complete: {total_records} records for {len(df_dict)} symbols", "#22c55e")

    def reset_fetch_button(self):
        self.progress_bar.setVisible(False)
        self.fetch_button.setEnabled(True)
        self.fetch_button.setText("Fetch Data")
        if self.df_dict:
            total_records = sum(len(df) for df in self.df_dict.values())
            self.status_bar.showMessage(f"Data fetched: {total_records} records • Ready for export")
        else:
            self.status_bar.showMessage("Ready to fetch data")

    def export_csv(self):
        if not self.df_dict:
            self.show_error("No data to export.")
            return

        selected_symbols = self.symbol_input.get_selected_symbols()
        if len(self.df_dict) < len(selected_symbols):
            failed_symbols = [s for s in selected_symbols if s not in self.df_dict]
            self.show_error(f"Cannot export: Missing data for {', '.join(failed_symbols)}")
            return

        base_dir = os.path.expanduser("~/Downloads/data")
        exchange = self.exchange_combo.currentText().strip().upper()
        exchange_dir = os.path.join(base_dir, exchange)
        
        os.makedirs(exchange_dir, exist_ok=True)
        
        exported_files = []
        
        for symbol, df in self.df_dict.items():
            df_cleaned = df.copy()
            
            if 'datetime' in df_cleaned.columns:
                try:
                    df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], errors='coerce').dt.strftime('%m/%d/%Y')
                except Exception as e:
                    self.show_error(f"Failed to format datetime for {symbol}: {str(e)}")
                    return
            elif 'candle_timestamp' in df_cleaned.columns:
                try:
                    df_cleaned['candle_timestamp'] = pd.to_datetime(df_cleaned['candle_timestamp'], errors='coerce').dt.strftime('%m/%d/%Y')
                except Exception as e:
                    self.show_error(f"Failed to format candle_timestamp for {symbol}: {str(e)}")
                    return
            else:
                self.show_error(f"No date column (datetime or candle_timestamp) found for {symbol}.")
                return

            if 'symbol' in df_cleaned.columns:
                df_cleaned['symbol'] = df_cleaned['symbol'].str.replace(f"{exchange}:", "", regex=False)

            filename = os.path.join(exchange_dir, f"{symbol}.csv")
            try:
                df_cleaned.to_csv(filename, index=False)
                exported_files.append(os.path.basename(filename))
                self.add_log(f"Exported {len(df_cleaned)} rows for {symbol} to {filename}", "#22c55e")
            except Exception as e:
                self.show_error(f"Failed to export data for {symbol}: {str(e)}")
                return
        
        self.status_bar.showMessage(f"Data exported for {len(exported_files)} symbols")
        self.show_info("Export Successful", 
                      f"Data exported to:\n{exchange_dir}\n\nFiles: {', '.join(exported_files)}")

    def show_error(self, message):
        self.add_log(message, "#ef4444")
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("Error")
        msg.setText(message)
        msg.setStyleSheet(self.styleSheet())
        msg.exec()

    def show_info(self, title, message):
        self.add_log(message, "#22c55e")
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle(title)
        msg.setText(message)
        msg.setStyleSheet(self.styleSheet())
        msg.exec()

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("TradingView Data")
    app.setApplicationVersion("2.1")
    
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