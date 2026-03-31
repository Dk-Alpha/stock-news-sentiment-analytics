import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox
import requests
import yaml
import threading
import time
import json
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# ─── Theme ────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# Load .env from the ui/ directory
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
# sources.yaml lives in the pipeline/ sibling directory
SOURCES_FILE = Path(__file__).parent.parent / "pipeline" / "sources.yaml"

SENTIMENT_COLOR = {
    "positive": "#22c55e",
    "negative": "#ef4444",
    "neutral":  "#94a3b8",
    None:       "#64748b",
}
SENTIMENT_ICON = {
    "positive": "▲",
    "negative": "▼",
    "neutral":  "●",
    None:       "○",
}


def load_sources():
    with open(SOURCES_FILE, "r") as f:
        return yaml.safe_load(f).get("sources", [])

def save_sources(sources):
    with open(SOURCES_FILE, "w") as f:
        yaml.dump({"sources": sources}, f, default_flow_style=False, sort_keys=False)


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("📡 Live News Intelligence Dashboard")
        self.geometry("1380x820")
        self.minsize(1100, 650)
        self.configure(fg_color="#0f172a")

        self._news_items = []
        self._selected_ticker = tk.StringVar(value="ALL")
        self._api_status = tk.StringVar(value="⚪ Connecting...")
        self._stream_running = False

        self._build_ui()
        self._start_stream()

    # ──────────────────────────────────────────────────────────────
    # UI Construction
    # ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Top Bar ──
        topbar = ctk.CTkFrame(self, fg_color="#1e293b", height=56, corner_radius=0)
        topbar.pack(fill="x", side="top")
        topbar.pack_propagate(False)

        ctk.CTkLabel(topbar, text="📡  Live News Intelligence", font=("Inter", 20, "bold"),
                     text_color="#f1f5f9").pack(side="left", padx=20, pady=10)

        ctk.CTkLabel(topbar, textvariable=self._api_status, font=("Inter", 12),
                     text_color="#94a3b8").pack(side="right", padx=20)

        date_label = ctk.CTkLabel(topbar, text=datetime.now().strftime("%A, %B %d %Y"),
                                  font=("Inter", 12), text_color="#64748b")
        date_label.pack(side="right", padx=10)

        # ── Main Body ──
        body = ctk.CTkFrame(self, fg_color="#0f172a")
        body.pack(fill="both", expand=True, padx=12, pady=(8, 12))
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        # Left sidebar
        self._build_sidebar(body)

        # Right content
        self._build_content(body)

    def _build_sidebar(self, parent):
        sidebar = ctk.CTkFrame(parent, fg_color="#1e293b", width=260, corner_radius=12)
        sidebar.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        sidebar.pack_propagate(False)

        # ── Sentiment Summary ──
        ctk.CTkLabel(sidebar, text="SENTIMENT SUMMARY", font=("Inter", 10, "bold"),
                     text_color="#475569").pack(anchor="w", padx=16, pady=(16, 4))

        self._summary_frame = ctk.CTkScrollableFrame(sidebar, fg_color="transparent", height=200)
        self._summary_frame.pack(fill="x", padx=8)

        # ── Filter ──
        ctk.CTkLabel(sidebar, text="FILTER BY TICKER", font=("Inter", 10, "bold"),
                     text_color="#475569").pack(anchor="w", padx=16, pady=(20, 4))

        self._ticker_filter_var = tk.StringVar(value="ALL")
        self._ticker_menu = ctk.CTkOptionMenu(sidebar, variable=self._ticker_filter_var,
                                              values=["ALL"],
                                              command=self._on_filter_change,
                                              fg_color="#334155", button_color="#3b82f6",
                                              font=("Inter", 12))
        self._ticker_menu.pack(fill="x", padx=16)

        # ── Manage Sources ──
        ctk.CTkLabel(sidebar, text="NEWS SOURCES", font=("Inter", 10, "bold"),
                     text_color="#475569").pack(anchor="w", padx=16, pady=(20, 4))

        ctk.CTkButton(sidebar, text="⚙  Manage Sources", font=("Inter", 12),
                      fg_color="#334155", hover_color="#475569",
                      command=self._open_sources_manager).pack(fill="x", padx=16)

        # ── Refresh ──
        ctk.CTkButton(sidebar, text="↻  Refresh Now", font=("Inter", 12),
                      fg_color="#1d4ed8", hover_color="#2563eb",
                      command=self._manual_refresh).pack(fill="x", padx=16, pady=(8, 0))

    def _build_content(self, parent):
        content = ctk.CTkFrame(parent, fg_color="transparent")
        content.grid(row=0, column=1, sticky="nsew")
        content.rowconfigure(1, weight=1)
        content.columnconfigure(0, weight=1)

        # Stats bar
        self._stats_bar = ctk.CTkFrame(content, fg_color="#1e293b", height=52, corner_radius=10)
        self._stats_bar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        self._stats_bar.pack_propagate(False)

        self._stat_total = self._stat_chip(self._stats_bar, "Total", "0")
        self._stat_positive = self._stat_chip(self._stats_bar, "Positive ▲", "0", "#22c55e")
        self._stat_negative = self._stat_chip(self._stats_bar, "Negative ▼", "0", "#ef4444")
        self._stat_neutral = self._stat_chip(self._stats_bar, "Neutral ●", "0", "#94a3b8")
        self._stat_pending = self._stat_chip(self._stats_bar, "Pending ○", "0", "#64748b")

        # News feed
        self._news_frame = ctk.CTkScrollableFrame(content, fg_color="#0f172a")
        self._news_frame.grid(row=1, column=0, sticky="nsew")

    def _stat_chip(self, parent, label, value, color="#f1f5f9"):
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(side="left", padx=20)
        val_label = ctk.CTkLabel(frame, text=value, font=("Inter", 18, "bold"), text_color=color)
        val_label.pack()
        ctk.CTkLabel(frame, text=label, font=("Inter", 9), text_color="#64748b").pack()
        return val_label

    # ──────────────────────────────────────────────────────────────
    # Streaming & Data Refresh
    # ──────────────────────────────────────────────────────────────

    def _start_stream(self):
        self._stream_running = True
        t = threading.Thread(target=self._poll_loop, daemon=True)
        t.start()

    def _poll_loop(self):
        """Background thread: polls API every 8 seconds."""
        while self._stream_running:
            self._fetch_and_render()
            time.sleep(8)

    def _fetch_and_render(self):
        try:
            ticker = self._ticker_filter_var.get()
            params = {} if ticker == "ALL" else {"ticker": ticker}

            resp = requests.get(f"{API_BASE}/news", params={**params, "limit": 200}, timeout=5)
            resp.raise_for_status()
            news = resp.json()

            summary_resp = requests.get(f"{API_BASE}/news/summary", params=params, timeout=5)
            summary = summary_resp.json() if summary_resp.ok else []

            tickers_resp = requests.get(f"{API_BASE}/tickers", timeout=5)
            tickers = ["ALL"] + (tickers_resp.json() if tickers_resp.ok else [])

            self.after(0, lambda: self._update_ui(news, summary, tickers))
            self.after(0, lambda: self._api_status.set("🟢 Live"))
        except Exception:
            self.after(0, lambda: self._api_status.set("🔴 API Offline"))

    def _update_ui(self, news, summary, tickers):
        self._update_stats(news)
        self._update_ticker_menu(tickers)
        self._update_summary_panel(summary)
        self._render_news_feed(news)

    def _update_stats(self, news):
        total = len(news)
        pos = sum(1 for n in news if n.get("sentiment") == "positive")
        neg = sum(1 for n in news if n.get("sentiment") == "negative")
        neu = sum(1 for n in news if n.get("sentiment") == "neutral")
        pending = sum(1 for n in news if not n.get("sentiment"))
        self._stat_total.configure(text=str(total))
        self._stat_positive.configure(text=str(pos))
        self._stat_negative.configure(text=str(neg))
        self._stat_neutral.configure(text=str(neu))
        self._stat_pending.configure(text=str(pending))

    def _update_ticker_menu(self, tickers):
        current = self._ticker_filter_var.get()
        self._ticker_menu.configure(values=tickers)
        if current not in tickers:
            self._ticker_filter_var.set("ALL")

    def _update_summary_panel(self, summary):
        for w in self._summary_frame.winfo_children():
            w.destroy()
        for row in summary:
            ticker = row["company_ticker"]
            pos = row.get("positive", 0) or 0
            neg = row.get("negative", 0) or 0
            total = row.get("total", 1) or 1
            pct = int((pos / total) * 100)

            card = ctk.CTkFrame(self._summary_frame, fg_color="#0f172a", corner_radius=8)
            card.pack(fill="x", pady=3)
            ctk.CTkLabel(card, text=ticker, font=("Inter", 12, "bold"),
                         text_color="#f1f5f9").pack(anchor="w", padx=10, pady=(6, 0))
            bar_frame = ctk.CTkFrame(card, fg_color="#1e293b", height=6, corner_radius=3)
            bar_frame.pack(fill="x", padx=10, pady=4)
            bar_frame.pack_propagate(False)
            bar = ctk.CTkFrame(bar_frame, fg_color="#22c55e", height=6,
                               width=int(2.2 * pct), corner_radius=3)
            bar.place(x=0, y=0, relheight=1)
            ctk.CTkLabel(card, text=f"▲{pos}  ▼{neg}  {pct}% positive",
                         font=("Inter", 10), text_color="#94a3b8").pack(anchor="w", padx=10, pady=(0, 6))

    def _render_news_feed(self, news):
        for w in self._news_frame.winfo_children():
            w.destroy()

        if not news:
            ctk.CTkLabel(self._news_frame, text="No news yet — pipeline is warming up...",
                         font=("Inter", 14), text_color="#475569").pack(pady=40)
            return

        for item in news:
            self._news_card(item)

    def _news_card(self, item):
        sentiment = item.get("sentiment")
        color = SENTIMENT_COLOR[sentiment]
        icon = SENTIMENT_ICON[sentiment]
        ticker = item.get("company_ticker", "")
        title = item.get("title", "")
        source = item.get("source_name", "")
        ingested = item.get("ingested_at", "")[:19].replace("T", " ") if item.get("ingested_at") else ""
        confidence = item.get("confidence_score")
        reasoning = item.get("reasoning", "")

        card = ctk.CTkFrame(self._news_frame, fg_color="#1e293b", corner_radius=10)
        card.pack(fill="x", pady=4)

        # Left accent strip
        accent = ctk.CTkFrame(card, fg_color=color, width=4, corner_radius=0)
        accent.pack(side="left", fill="y")

        body = ctk.CTkFrame(card, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=12, pady=8)

        # Header row: icon + ticker + source + timestamp
        header = ctk.CTkFrame(body, fg_color="transparent")
        header.pack(fill="x")
        ctk.CTkLabel(header, text=f"{icon} {ticker}", font=("Inter", 12, "bold"),
                     text_color=color).pack(side="left")
        ctk.CTkLabel(header, text=source, font=("Inter", 10),
                     text_color="#475569").pack(side="left", padx=12)
        ctk.CTkLabel(header, text=ingested, font=("Inter", 10),
                     text_color="#334155").pack(side="right")

        # Confidence badge
        if confidence is not None:
            badge_text = f"{int(confidence * 100)}% confidence"
            ctk.CTkLabel(header, text=badge_text, font=("Inter", 10),
                         text_color="#64748b").pack(side="right", padx=8)

        # Title
        ctk.CTkLabel(body, text=title, font=("Inter", 13),
                     text_color="#e2e8f0", wraplength=900, justify="left").pack(anchor="w", pady=(4, 0))

        # Reasoning
        if reasoning:
            ctk.CTkLabel(body, text=reasoning, font=("Inter", 11, "italic"),
                         text_color="#64748b", wraplength=900, justify="left").pack(anchor="w")

    # ──────────────────────────────────────────────────────────────
    # Controls
    # ──────────────────────────────────────────────────────────────

    def _on_filter_change(self, value):
        self._fetch_and_render()

    def _manual_refresh(self):
        threading.Thread(target=self._fetch_and_render, daemon=True).start()

    def _open_sources_manager(self):
        win = SourcesManagerWindow(self)
        win.grab_set()

    def on_closing(self):
        self._stream_running = False
        self.destroy()


# ─── Sources Manager Window ──────────────────────────────────────────────────

class SourcesManagerWindow(ctk.CTkToplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Manage News Sources")
        self.geometry("700x540")
        self.configure(fg_color="#0f172a")
        self._sources = load_sources()
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="📰 News Sources Configuration",
                     font=("Inter", 16, "bold"), text_color="#f1f5f9").pack(pady=(16, 4))
        ctk.CTkLabel(self, text="Changes take effect on the next ingester polling cycle.",
                     font=("Inter", 11), text_color="#475569").pack(pady=(0, 12))

        self._list_frame = ctk.CTkScrollableFrame(self, fg_color="#1e293b", corner_radius=10)
        self._list_frame.pack(fill="both", expand=True, padx=16)

        self._render_list()

        add_frame = ctk.CTkFrame(self, fg_color="transparent")
        add_frame.pack(fill="x", padx=16, pady=12)

        self._new_name = ctk.CTkEntry(add_frame, placeholder_text="Source Name", width=150)
        self._new_name.grid(row=0, column=0, padx=4)
        self._new_ticker = ctk.CTkEntry(add_frame, placeholder_text="TICKER", width=80)
        self._new_ticker.grid(row=0, column=1, padx=4)
        self._new_url = ctk.CTkEntry(add_frame, placeholder_text="RSS Feed URL", width=300)
        self._new_url.grid(row=0, column=2, padx=4)
        ctk.CTkButton(add_frame, text="+ Add", fg_color="#1d4ed8", hover_color="#2563eb",
                      command=self._add_source).grid(row=0, column=3, padx=4)

    def _render_list(self):
        for w in self._list_frame.winfo_children():
            w.destroy()

        for i, src in enumerate(self._sources):
            row = ctk.CTkFrame(self._list_frame, fg_color="#0f172a", corner_radius=8)
            row.pack(fill="x", pady=3)

            ctk.CTkLabel(row, text=src.get("company_ticker", ""), font=("Inter", 11, "bold"),
                         text_color="#3b82f6", width=60).pack(side="left", padx=8)
            ctk.CTkLabel(row, text=src.get("name", ""), font=("Inter", 11),
                         text_color="#e2e8f0", width=160).pack(side="left", padx=4)
            ctk.CTkLabel(row, text=src.get("url", ""), font=("Inter", 10),
                         text_color="#64748b", wraplength=320).pack(side="left", padx=4, fill="x", expand=True)

            ctk.CTkButton(row, text="Remove", font=("Inter", 10), width=70,
                          fg_color="#7f1d1d", hover_color="#991b1b",
                          command=lambda idx=i: self._remove_source(idx)).pack(side="right", padx=8, pady=4)

    def _add_source(self):
        name = self._new_name.get().strip()
        ticker = self._new_ticker.get().strip().upper()
        url = self._new_url.get().strip()
        if not (name and ticker and url):
            messagebox.showerror("Validation", "All fields are required.")
            return
        self._sources.append({"name": name, "url": url, "type": "rss", "company_ticker": ticker})
        save_sources(self._sources)
        self._new_name.delete(0, "end")
        self._new_ticker.delete(0, "end")
        self._new_url.delete(0, "end")
        self._render_list()

    def _remove_source(self, idx):
        del self._sources[idx]
        save_sources(self._sources)
        self._render_list()


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
