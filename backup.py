from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QTableWidget, QTableWidgetItem, 
    QFileDialog, QFrame, QMessageBox, QProgressBar, QSplashScreen, QStatusBar, QListWidget, QListWidgetItem, QCheckBox, QDateEdit
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QPropertyAnimation, QEasingCurve, QDateTime, QDate
from PySide6.QtGui import QFont, QPalette, QColor, QLinearGradient, QBrush, QPixmap, QPainter, QIcon
import sys
import pandas as pd
import os
from datetime import datetime

try:
    # from main import TvDatafeed, Interval
    from main_backup import TvDatafeed, Interval
except ImportError:
    print("Error: Could not import TvDatafeed and Interval from main.py")
    print("Please ensure main.py is in the same directory")
    sys.exit(1)

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
        
        painter.setPen(QColor("#a1a1aa"))
        painter.setFont(QFont("Inter", 16, QFont.Normal))
        subtitle_rect = pixmap.rect().adjusted(0, 20, 0, 0)
        
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

class DataFetchThread(QThread):
    data_fetched = Signal(dict)
    error = Signal(str)
    progress = Signal(int)
    symbol_fetched = Signal(str)

    def __init__(self, tv, symbols, exchange, interval, n_bars):
        super().__init__()
        self.tv = tv
        self.symbols = symbols
        self.exchange = exchange
        self.interval = interval
        self.n_bars = n_bars

    def run(self):
        try:
            results = {}
            total_symbols = len(self.symbols)
            for i, symbol in enumerate(self.symbols):
                self.progress.emit(int((i / total_symbols) * 100))
                df = self.tv.get_hist(
                    symbol=symbol,
                    exchange=self.exchange,
                    interval=self.interval,
                    n_bars=self.n_bars
                )
                if df is not None and not df.empty:
                    # Reset index to move datetime to a column
                    df = df.reset_index()
                    if 'timestamp' in df.columns:
                        df = df.rename(columns={'timestamp': 'datetime'})
                    # Clean symbol column by removing exchange prefix
                    if 'symbol' in df.columns:
                        df['symbol'] = df['symbol'].str.replace(f"{self.exchange}:", "", regex=False)
                    results[symbol] = df
                    self.symbol_fetched.emit(f"{symbol} data downloaded")
                self.progress.emit(int(((i + 1) / total_symbols) * 100))
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
    def __init__(self, items):
        super().__init__()
        self.items = items
        self.selected_items = []
        
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        
        # Label
        label = QLabel("Select Symbols")
        label.setStyleSheet("font-size: 15px; font-weight: 300; color: #a1a1aa; margin-bottom: 8px;")
        layout.addWidget(label)
        
        # QComboBox to trigger the dropdown
        self.combo_box = QComboBox()
        self.combo_box.setFixedHeight(48)
        self.combo_box.setFixedWidth(450)
        self.combo_box.setStyleSheet("""
            QComboBox {
                background-color: #374151;
                border: 1px solid #4b5563;
                border-radius: 10px;
                padding: 12px 16px;
                color: #ffffff;
                font-size: 16px;
                font-family: Inter;
            }
            QComboBox:focus {
                border-color: #60a5fa;
                outline: none;
            }
            QComboBox::drop-down {
                border: none;
            }
        """)
        
        # Make the QComboBox editable to allow typing
        self.combo_box.setEditable(True)
        self.combo_box.setInsertPolicy(QComboBox.NoInsert)
        self.combo_box.lineEdit().setPlaceholderText("Type to filter symbols...")
        
        # Select All Button
        button_layout = QHBoxLayout()
        self.select_all_button = ModernButton("Select All", primary=False)
        self.select_all_button.clicked.connect(self.select_all_symbols)
        button_layout.addWidget(self.select_all_button)
        button_layout.addStretch()
        
        # QListWidget for the dropdown content
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
        
        # Store original items for filtering
        self.all_checkboxes = []
        
        # Add checkboxes for each symbol
        for item in items:
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
        
        # Set the QListWidget as the view for the QComboBox
        self.combo_box.setView(self.list_widget)
        self.combo_box.setModel(self.list_widget.model())
        
        # Connect the text edit signal to filter the list
        self.combo_box.lineEdit().textEdited.connect(self.filter_symbols)
        
        self.update_combo_display()
        layout.addWidget(self.combo_box)
        layout.addLayout(button_layout)
    
    def filter_symbols(self, text):
        text = text.lower()
        for list_item, checkbox in self.all_checkboxes:
            symbol = checkbox.text().lower()
            list_item.setHidden(text not in symbol)
    
    def on_checkbox_changed(self):
        self.selected_items = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            checkbox = self.list_widget.itemWidget(item)
            if checkbox.isChecked() and not item.isHidden():
                self.selected_items.append(checkbox.text())
        self.update_combo_display()
    
    def select_all_symbols(self):
        for list_item, checkbox in self.all_checkboxes:
            if not list_item.isHidden():
                checkbox.setChecked(True)
        self.on_checkbox_changed()
    
    def update_combo_display(self):
        if self.selected_items:
            self.combo_box.lineEdit().setText(", ".join(self.selected_items))
        else:
            self.combo_box.lineEdit().setText("")
            self.combo_box.lineEdit().setPlaceholderText("Type to filter symbols...")
    
    def get_selected_symbols(self):
        return self.selected_items

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Downloader")
        self.setMinimumSize(1200, 800)
        self.df_dict = {}  # Store DataFrames for each symbol
            
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
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
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
            
            QTableWidget {
                background-color: transparent;
                alternate-background-color: transparent;
                border: 1px solid #4b5563;
                border-radius: 12px;
                gridline-color: #4b5563;
                color: #ffffff;
                font-size: 15px;
                font-family: Inter;
                selection-background-color: #2563eb;
                selection-color: white;
            }
            QTableWidget::item {
                background-color: transparent;
                padding: 12px 16px;
                border: none;
                border-bottom: 1px solid #4b5563;
            }
            QTableWidget::item:selected {
                background-color: #2563eb;
                color: white;
            }
            QTableWidget::item:hover {
                background-color: #4b5563;
            }
            QHeaderView {
                background-color: transparent;
            }
            QHeaderView::section {
                background-color: #374151;
                color: #a1a1aa;
                padding: 16px;
                border: none;
                border-bottom: 1px solid #4b5563;
                border-right: 1px solid #4b5563;
                font-weight: 600;
                font-size: 13px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            QHeaderView::section:first {
                border-top-left-radius: 12px;
            }
            QHeaderView::section:last {
                border-top-right-radius: 12px;
                border-right: none;
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
            QStatusBar::item {
                border: none;
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

        self.exchanges = [
            "NSE", "BSE", "MCX", "NCDEX", "CDS",
            "NASDAQ", "NYSE", "AMEX", "OTC",
            "LSE", "EURONEXT", "XETRA",
            "TSE", "OSE", "HKEX", "SSE", "SZSE",
            "TSX", "TSXV",
            "ASX",
            "JSE",
            "MOEX",
            "BOVESPA",
            "BMV",
            "TADAWUL",
            "TASE",
            "KRX",
            "TWSE",
            "SGX",
            "SET",
            "IDX",
            "KLSE",
            "PSE",
            "VNX",
        ]

        self.symbols = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR",
            "ICICIBANK", "KOTAKBANK", "SBIN", "BAJFINANCE", "BHARTIARTL",
            "ASIANPAINT", "ITC", "HCLTECH", "AXISBANK", "MARUTI",
            "SUNPHARMA", "WIPRO", "ULTRACEMCO", "TITAN", "ADANIENT"
        ]
        self.init_ui()
        
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
        self.symbol_input = SymbolSelector(self.symbols)
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

        table_card = QFrame()
        table_card.setObjectName("card")
        table_layout = QVBoxLayout()
        table_card.setLayout(table_layout)

        table_header = QLabel("Market Data")
        table_header.setObjectName("section")
        table_layout.addWidget(table_header)

        self.table = QTableWidget()
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setShowGrid(False)
        self.table.setFocusPolicy(Qt.NoFocus)
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.table.setSortingEnabled(True)
        
        self.table.verticalHeader().setDefaultSectionSize(48)
        self.table.verticalHeader().setVisible(False)
        
        self.table.horizontalHeader().setMinimumSectionSize(120)
        table_layout.addWidget(self.table)
        
        main_layout.addWidget(table_card, 1)

        self.status_bar = QStatusBar()
        self.status_bar.setFixedHeight(36)
        self.status_bar.showMessage("Ready to fetch data")
        main_layout.addWidget(self.status_bar)

        self.fetch_button.clicked.connect(self.fetch_data)
        self.export_button.clicked.connect(self.export_csv)
        
        self.exchange_combo.setCurrentText("NSE")

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
        self.status_bar.showMessage(f"Fetching data for {', '.join(symbols)} from {exchange}")

        try:
            tv = TvDatafeed()
            self.df_dict = {}
            self.thread = DataFetchThread(tv, symbols, exchange, interval_enum, n_bars)
            self.thread.data_fetched.connect(self.display_data)
            self.thread.error.connect(self.show_error)
            self.thread.progress.connect(self.progress_bar.setValue)
            self.thread.symbol_fetched.connect(self.update_status_bar)
            self.thread.finished.connect(self.reset_fetch_button)
            self.thread.start()
        except Exception as e:
            self.show_error(f"Failed to initialize data fetcher: {str(e)}")
            self.reset_fetch_button()

    def update_status_bar(self, message):
        self.status_bar.showMessage(message)

    def reset_fetch_button(self):
        self.progress_bar.setVisible(False)
        self.fetch_button.setEnabled(True)
        self.fetch_button.setText("Fetch Data")
        if self.df_dict:
            total_records = sum(len(df) for df in self.df_dict.values())
            self.status_bar.showMessage(f"Data loaded: {total_records} records • Ready for export")
        else:
            self.status_bar.showMessage("Ready to fetch data")

    def display_data(self, df_dict):
        self.df_dict = df_dict
        if not df_dict:
            self.show_error("No data returned. Please check the symbols, exchange, or your connection.")
            self.export_button.setEnabled(False)
            return

        # Combine all DataFrames for display
        combined_df = pd.concat(
            [df.assign(symbol=symbol) for symbol, df in df_dict.items()],
            ignore_index=True
        )

        if combined_df.empty:
            self.show_error("No data returned for selected symbols.")
            self.export_button.setEnabled(False)
            return

        # Format datetime as MM/DD/YYYY
        if 'datetime' in combined_df.columns:
            combined_df['datetime'] = pd.to_datetime(combined_df['datetime'], errors='coerce').dt.strftime('%m/%d/%Y')
        elif 'candle_timestamp' in combined_df.columns:
            combined_df['candle_timestamp'] = pd.to_datetime(combined_df['candle_timestamp'], errors='coerce').dt.strftime('%m/%d/%Y')
        else:
            self.show_error("No date column (datetime or candle_timestamp) found in the data.")
            self.export_button.setEnabled(False)
            return

        # Clean symbol column
        if 'symbol' in combined_df.columns:
            combined_df['symbol'] = combined_df['symbol'].str.replace(f"{self.exchange_combo.currentText().upper()}:", "", regex=False)

        self.table.setRowCount(combined_df.shape[0])
        self.table.setColumnCount(combined_df.shape[1])
        
        headers = []
        for col in combined_df.columns:
            if col.lower() == 'datetime' or col.lower() == 'candle_timestamp':
                headers.append('Date')
            elif col.lower() in ['open', 'high', 'low', 'close']:
                headers.append(col.title())
            elif col.lower() == 'volume':
                headers.append('Volume')
            elif col.lower() == 'symbol':
                headers.append('Symbol')
            else:
                headers.append(col.title())
        
        self.table.setHorizontalHeaderLabels(headers)
        
        for row_idx, (_, row) in enumerate(combined_df.iterrows()):
            # Set row header as row number
            self.table.setVerticalHeaderItem(row_idx, QTableWidgetItem(str(row_idx + 1)))
            
            for col_idx, value in enumerate(row):
                if combined_df.columns[col_idx].lower() in ['datetime', 'candle_timestamp']:
                    formatted_value = str(value)  # Already formatted as MM/DD/YYYY
                elif combined_df.columns[col_idx].lower() in ['open', 'high', 'low', 'close']:
                    try:
                        formatted_value = f"₹{float(value):.2f}"
                    except:
                        formatted_value = str(value)
                elif combined_df.columns[col_idx].lower() == 'volume':
                    try:
                        formatted_value = f"{int(float(value)):,}"
                    except:
                        formatted_value = str(value)
                else:
                    formatted_value = str(value)
                
                item = QTableWidgetItem(formatted_value)
                item.setTextAlignment(Qt.AlignCenter)
                
                if combined_df.columns[col_idx].lower() in ['open', 'high', 'low', 'close'] and row_idx > 0:
                    try:
                        current_val = float(value)
                        prev_val = float(combined_df.iloc[row_idx-1, col_idx])
                        if current_val > prev_val:
                            item.setForeground(QColor("#ffffff"))  # Green for increase
                        elif current_val < prev_val:
                            item.setForeground(QColor("#ffffff"))  # Red for decrease
                        else:
                            item.setForeground(QColor("#ffffff"))
                    except:
                        item.setForeground(QColor("#ffffff"))
                else:
                    item.setForeground(QColor("#ffffff"))
                
                self.table.setItem(row_idx, col_idx, item)
        
        self.table.resizeColumnsToContents()
        for col in range(self.table.columnCount()):
            if self.table.columnWidth(col) < 140:
                self.table.setColumnWidth(col, 140)
        
        self.export_button.setEnabled(True)
        
        symbols = self.symbol_input.get_selected_symbols()
        exchange = self.exchange_combo.currentText().upper()
        interval = self.interval_input.currentText()
        total_records = len(combined_df)
        
        price_change_info = ""
        if 'close' in combined_df.columns and len(combined_df) > 1:
            try:
                latest_price = float(combined_df.iloc[0]['close'])
                previous_price = float(combined_df.iloc[1]['close'])
                change = latest_price - previous_price
                change_percent = (change / previous_price) * 100
                direction = "up" if change >= 0 else "down"
                price_change_info = f" • Latest: ₹{latest_price:.2f} ({change:+.2f}, {change_percent:+.1f}%)"
            except:
                pass
        
        date_range = ""
        date_column = 'datetime' if 'datetime' in combined_df.columns else 'candle_timestamp' if 'candle_timestamp' in combined_df.columns else None
        if not combined_df.empty and date_column:
            try:
                start_date = str(combined_df.iloc[-1][date_column])
                end_date = str(combined_df.iloc[0][date_column])
                date_range = f"\nDate Range: {start_date} to {end_date}"
            except:
                pass
        
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.show_info("Data Fetched Successfully", f"Successfully fetched {total_records} records for {', '.join(symbols)}{date_range}\n\nFetched at: {fetch_time}")

    def export_csv(self):
        if not self.df_dict:
            self.show_error("No data to export.")
            return

        base_dir = os.path.expanduser("~/Downloads/data")
        exchange = self.exchange_combo.currentText().strip().upper()
        exchange_dir = os.path.join(base_dir, exchange)
        
        os.makedirs(exchange_dir, exist_ok=True)
        
        exported_files = []
        
        for symbol, df in self.df_dict.items():
            # Clean the DataFrame for export
            df_cleaned = df.copy()
            
            # Handle datetime or candle_timestamp column
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

            # Clean symbol column
            if 'symbol' in df_cleaned.columns:
                df_cleaned['symbol'] = df_cleaned['symbol'].str.replace(f"{exchange}:", "", regex=False)

            filename = os.path.join(exchange_dir, f"{symbol}.CSV")
            try:
                df_cleaned.to_csv(filename, index=False)
                exported_files.append(os.path.basename(filename))
            except Exception as e:
                self.show_error(f"Failed to export data for {symbol}: {str(e)}")
                return
        
        self.status_bar.showMessage(f"Data exported for {', '.join(self.df_dict.keys())}")
        self.show_info("Export Successful", f"Data exported successfully to:\n{exchange_dir}\n\nFiles: {', '.join(exported_files)}")

    def show_error(self, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("Error")
        msg.setText(message)
        msg.setStyleSheet(self.styleSheet())
        msg.exec()

    def show_info(self, title, message):
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
    
    import time
    time.sleep(1.5)
    
    window = MainWindow()
    
    splash.close()
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()