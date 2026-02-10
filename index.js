const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const app = express();
app.use(express.json());

// CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// 사용자별 WebSocket 구독 관리
const userSubscriptions = new Map(); // key: exchange-symbol-timeframe, value: Set<userId>
const activeConnections = new Map(); // key: exchange-symbol-timeframe, value: WebSocket

// Binance WebSocket 연결
function connectBinanceWS(exchange, symbol, timeframe, isTestnet) {
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  if (activeConnections.has(key)) {
    console.log(`✅ Already connected: ${key}`);
    return;
  }
  
  const baseUrl = isTestnet 
    ? 'wss://stream.binancefuture.com'
    : 'wss://fstream.binance.com';
  
  const stream = `${symbol.toLowerCase()}@kline_${timeframe}`;
  const wsUrl = `${baseUrl}/ws/${stream}`;
  
  console.log(`🔌 Connecting to: ${wsUrl}`);
  
  const ws = new WebSocket(wsUrl);
  
  ws.on('open', () => {
    console.log(`✅ Connected: ${key}`);
    activeConnections.set(key, ws);
  });
  
  ws.on('message', (data) => {
    try {
      const parsed = JSON.parse(data);
      if (parsed.e === 'kline' && parsed.k.x) {
        const candle = {
          exchange: exchange,
          symbol: symbol,
          timeframe: timeframe,
          timestamp: parsed.k.T,
          open: parseFloat(parsed.k.o),
          high: parseFloat(parsed.k.h),
          low: parseFloat(parsed.k.l),
          close: parseFloat(parsed.k.c),
          volume: parseFloat(parsed.k.v)
        };
        
        console.log(`📊 Candle sent: ${key} ${candle.close}`);
        
        // 이 symbol/timeframe을 구독 중인 모든 사용자에게 전송
        const users = userSubscriptions.get(key);
        if (users && users.size > 0) {
          for (const userId of users) {
            sendCandleToWorker(candle, userId);
          }
        }
      }
    } catch (error) {
      console.error('❌ Parse error:', error);
    }
  });
  
  ws.on('error', (error) => {
    console.error(`❌ WebSocket error (${key}):`, error);
  });
  
  ws.on('close', () => {
    console.log(`🔌 Disconnected: ${key}`);
    activeConnections.delete(key);
    
    // 재연결 (5초 후)
    setTimeout(() => {
      const users = userSubscriptions.get(key);
      if (users && users.size > 0) {
        console.log(`🔄 Reconnecting: ${key}`);
        connectBinanceWS(exchange, symbol, timeframe, isTestnet);
      }
    }, 5000);
  });
}

// Worker로 캔들 전송
async function sendCandleToWorker(candle, userId) {
  try {
    const response = await fetch('https://cointop10-forward.cointop10-com.workers.dev/api/new-candle', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-User-ID': userId
      },
      body: JSON.stringify(candle)
    });
    
    if (!response.ok) {
      console.error(`❌ Worker response error (user ${userId}):`, response.status);
    }
  } catch (error) {
    console.error(`❌ Send to worker failed (user ${userId}):`, error.message);
  }
}

// 연결 요청
app.post('/connect', (req, res) => {
  const { exchange, symbol, timeframe, userId } = req.body;
  
  if (!exchange || !symbol || !timeframe || !userId) {
    return res.status(400).json({ 
      success: false, 
      error: 'Missing required fields: exchange, symbol, timeframe, userId' 
    });
  }
  
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  // 사용자 구독 등록
  if (!userSubscriptions.has(key)) {
    userSubscriptions.set(key, new Set());
  }
  userSubscriptions.get(key).add(userId);
  
  console.log(`👤 User ${userId} subscribed to ${key}`);
  console.log(`📊 Total subscribers for ${key}: ${userSubscriptions.get(key).size}`);
  
  // WebSocket 연결 (없으면 생성)
  if (!activeConnections.has(key)) {
    const isTestnet = exchange.includes('testnet');
    const baseExchange = exchange.replace('-testnet', '');
    
    if (baseExchange === 'binance') {
      connectBinanceWS(exchange, symbol, timeframe, isTestnet);
    } else {
      return res.status(400).json({ 
        success: false, 
        error: `Exchange ${baseExchange} not supported yet` 
      });
    }
  }
  
  res.json({ success: true, message: `Connected to ${key}` });
});

// 연결 해제
app.post('/disconnect', (req, res) => {
  const { exchange, symbol, timeframe, userId } = req.body;
  
  if (!exchange || !symbol || !timeframe || !userId) {
    return res.status(400).json({ 
      success: false, 
      error: 'Missing required fields' 
    });
  }
  
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  // 사용자 구독 해제
  if (userSubscriptions.has(key)) {
    userSubscriptions.get(key).delete(userId);
    console.log(`👤 User ${userId} unsubscribed from ${key}`);
    
    // 더 이상 구독자가 없으면 WebSocket 종료
    if (userSubscriptions.get(key).size === 0) {
      userSubscriptions.delete(key);
      
      if (activeConnections.has(key)) {
        activeConnections.get(key).close();
        activeConnections.delete(key);
        console.log(`🔌 WebSocket closed: ${key} (no subscribers)`);
      }
    }
  }
  
  res.json({ success: true, message: `Disconnected from ${key}` });
});

// Binance API 프록시
app.post('/proxy/binance', async (req, res) => {
  try {
    const { url, method, headers } = req.body;
    
    const response = await fetch(url, {
      method: method,
      headers: headers
    });
    
    const data = await response.text();
    res.status(response.status).send(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 상태 확인
app.get('/status', (req, res) => {
  const status = {
    activeConnections: Array.from(activeConnections.keys()),
    userSubscriptions: {}
  };
  
  for (const [key, users] of userSubscriptions.entries()) {
    status.userSubscriptions[key] = users.size;
  }
  
  res.json(status);
});

app.get('/', (req, res) => {
  res.send('CoinTop10 WebSocket Bridge - Multi-User Support');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Railway WebSocket Bridge running on port ${PORT}`);
  console.log('✅ Multi-user support enabled');
});
