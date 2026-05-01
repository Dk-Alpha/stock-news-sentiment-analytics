import React, { useState, useEffect, useRef } from 'react';
import { Line } from 'react-chartjs-2';
import { 
  Chart as ChartJS, CategoryScale, LinearScale, PointElement, 
  LineElement, Title, Tooltip, Legend 
} from 'chart.js';
import { Activity, TrendingUp, TrendingDown, Minus, Clock, Zap } from 'lucide-react';
import './index.css';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

const API_BASE = 'http://localhost:8000';

function App() {
  const [news, setNews] = useState([]);
  const [summary, setSummary] = useState([]);
  const [tickers, setTickers] = useState(['ALL']);
  const [filterTicker, setFilterTicker] = useState('ALL');
  const [status, setStatus] = useState('connecting');
  const [chartData, setChartData] = useState({ labels: [], prices: [] });

  const eventSourceRef = useRef(null);

  // Initial Fetch
  useEffect(() => {
    fetchTickers();
    fetchSummary();
    fetchHistory();
  }, [filterTicker]);

  // SSE Connection
  useEffect(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const url = filterTicker === 'ALL' 
      ? `${API_BASE}/news/stream` 
      : `${API_BASE}/news/stream?ticker=${filterTicker}`;
      
    const sse = new EventSource(url);
    eventSourceRef.current = sse;

    sse.onmessage = (e) => {
      setStatus('connected');
      try {
        const data = JSON.parse(e.data);
        
        // Update Chart
        if (data.stock_price) {
          const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
          setChartData(prev => {
            const labels = [...prev.labels, timeStr].slice(-20);
            const prices = [...prev.prices, data.stock_price].slice(-20);
            return { labels, prices };
          });
        }

        if (data.heartbeat) return;

        // Update News
        setNews(prev => {
          const filtered = prev.filter(n => n.news_id !== data.news_id);
          return [data, ...filtered].slice(0, 500);
        });

        // Trigger fetch summary for fresh counts
        fetchSummary();
      } catch (err) {
        console.error(err);
      }
    };

    sse.onerror = () => setStatus('error');

    return () => sse.close();
  }, [filterTicker]);

  // API Methods
  const fetchTickers = async () => {
    try {
      const res = await fetch(`${API_BASE}/tickers`);
      const data = await res.json();
      setTickers(['ALL', ...data]);
    } catch (e) { console.error(e); }
  };

  const fetchSummary = async () => {
    try {
      const url = filterTicker === 'ALL' ? `${API_BASE}/news/summary` : `${API_BASE}/news/summary?ticker=${filterTicker}`;
      const res = await fetch(url);
      const data = await res.json();
      setSummary(data);
    } catch (e) { console.error(e); }
  };

  const fetchHistory = async () => {
    try {
      const url = filterTicker === 'ALL' ? `${API_BASE}/news?limit=100` : `${API_BASE}/news?ticker=${filterTicker}&limit=100`;
      const res = await fetch(url);
      const data = await res.json();
      setNews(data);
    } catch (e) { console.error(e); }
  };

  // Derived Stats
  const totals = summary.reduce((acc, curr) => {
    return {
      total: acc.total + parseInt(curr.total),
      pos: acc.pos + parseInt(curr.positive),
      neg: acc.neg + parseInt(curr.negative),
      neu: acc.neu + parseInt(curr.neutral),
    };
  }, { total: 0, pos: 0, neg: 0, neu: 0 });

  const classified = totals.pos + totals.neg + totals.neu;
  const pending = totals.total - classified;
  
  const getIndex = () => {
    if (classified === 0) return 50;
    return ((totals.pos + (totals.neu * 0.5)) / classified) * 100;
  };

  const posIndex = getIndex();

  // Chart Config
  const chartConfig = {
    data: {
      labels: chartData.labels,
      datasets: [{
        label: 'Price',
        data: chartData.prices,
        borderColor: '#fbbf24',
        backgroundColor: 'rgba(251, 191, 36, 0.1)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: { 
          position: 'right',
          grid: { color: 'rgba(255,255,255,0.05)' },
          ticks: { color: '#94a3b8', font: { family: 'Inter', size: 10 } }
        }
      }
    }
  };

  const SentimentIcon = ({ sentiment }) => {
    if (sentiment === 'positive') return <TrendingUp className="icon-emerald" size={16} />;
    if (sentiment === 'negative') return <TrendingDown className="icon-red" size={16} />;
    if (sentiment === 'neutral') return <Minus className="icon-slate" size={16} />;
    return <Clock className="icon-slate" size={16} />;
  };

  return (
    <div className="app-container">
      {/* Header */}
      <header className="header animate-fade-in">
        <div className="header-left">
          <div className="brand-accent"></div>
          <div>
            <h1 className="title">News Intelligence</h1>
            <p className="subtitle">AI Sentiment Pipeline</p>
          </div>
        </div>
        
        <div className="header-right">
          <select 
            value={filterTicker}
            onChange={e => setFilterTicker(e.target.value)}
            className="filter-select"
          >
            {tickers.map(t => <option key={t} value={t}>{t}</option>)}
          </select>

          <div className="status-badge glass-panel">
            <div className={`status-dot ${status === 'connected' ? 'connected animate-pulse-soft' : 'error'}`}></div>
            <span>{status === 'connected' ? 'Live Stream Active' : 'Connecting...'}</span>
          </div>
        </div>
      </header>

      {/* Main Grid */}
      <div className="dashboard-grid">
        
        {/* Total Stats */}
        <div className="glass-panel stat-card animate-fade-in" style={{animationDelay: '0.1s'}}>
          <h3 className="card-title">Today's Pipeline</h3>
          <div className="stat-big">{totals.total}</div>
          <p className="stat-sub">Articles Processed</p>
          
          <div className="stat-rows">
            <div className="stat-row">
              <span className="stat-label icon-emerald"><TrendingUp size={16}/> Bullish</span>
              <span className="stat-value">{totals.pos}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label icon-red"><TrendingDown size={16}/> Bearish</span>
              <span className="stat-value">{totals.neg}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label icon-slate"><Minus size={16}/> Neutral</span>
              <span className="stat-value">{totals.neu}</span>
            </div>
          </div>
        </div>

        {/* Index Gauge */}
        <div className="glass-panel stat-card animate-fade-in flex-col-between" style={{animationDelay: '0.2s'}}>
          <div>
            <h3 className="card-title">Positivity Index</h3>
            <div className="stat-huge text-gold">{posIndex.toFixed(1)}%</div>
            <p className="stat-sub mb-auto">Of classified news leans positive</p>
          </div>

          <div className="gauge-container">
            <div className="gauge-track">
              <div className="gauge-fill"></div>
              <div className="gauge-thumb" style={{ left: `${posIndex}%` }}></div>
            </div>
            <div className="gauge-labels">
              <span>Fear</span>
              <span>Greed</span>
            </div>
          </div>
        </div>

        {/* Price Action */}
        <div className="glass-panel chart-card animate-fade-in" style={{animationDelay: '0.3s'}}>
          <h3 className="card-title">Price Action Overlay</h3>
          <div className="chart-icon">
            <Zap className={status === 'connected' ? "text-gold animate-pulse-soft" : "icon-slate"} size={20} />
          </div>
          <div className="chart-wrapper">
            <Line data={chartConfig.data} options={chartConfig.options} />
          </div>
        </div>

      </div>

      {/* News Feed */}
      <div className="glass-panel feed-card animate-fade-in" style={{animationDelay: '0.4s'}}>
        <div className="feed-header">
          <h3 className="card-title">Live Inference Feed</h3>
          <div className="badge-count">{news.length} Items Loaded</div>
        </div>

        <div className="feed-list">
          {news.length === 0 ? (
            <div className="empty-state">Waiting for pipeline ingestion...</div>
          ) : (
            news.map((item, i) => {
              const sentClass = item.sentiment || 'pending';
              return (
                <div key={`${item.news_id}-${i}`} className={`news-item sentiment-${sentClass}`}>
                  <div className="news-top">
                    <div className="news-meta">
                      <span className="news-ticker">{item.company_ticker}</span>
                      <span className="news-source">{item.source_name || 'rss'}</span>
                    </div>
                    <div className="news-meta-right">
                      {item.confidence_score && (
                        <span className="news-conf">{Math.round(item.confidence_score * 100)}% Conf</span>
                      )}
                      <span className="news-time">{new Date(item.ingested_at).toLocaleTimeString()}</span>
                      <SentimentIcon sentiment={item.sentiment} />
                    </div>
                  </div>
                  
                  <h4 className="news-title">{item.title}</h4>
                  
                  {item.reasoning && (
                    <p className="news-reasoning">"{item.reasoning}"</p>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
