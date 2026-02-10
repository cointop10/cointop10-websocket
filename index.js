const express = require('express');
const WebSocket = require('ws');
const fetch = require('node-fetch');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

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
const activeConnections = new Map(); // key: exchange-symbol, value: WebSocket

// WebSocket 연결 (항상 1분봉만!)
function connectExchange(exchange, symbol) {
  const key = `${exchange}-${symbol}`;
  
  if (activeConnections.has(key)) {
    console.log(`✅ Already connected: ${key}`);
    return;
  }

  let wsUrl;
  
  switch (exchange.toLowerCase()) {
    case 'binance':
      wsUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_1m`;
      break;
    case 'binance-testnet':
      wsUrl = `wss://stream.binancefuture.com/ws/${symbol.toLowerCase()}@kline_1m`;
      break;
    case 'bybit':
      wsUrl = 'wss://stream.bybit.com/v5/public/linear';
      break;
    case 'bybit-testnet':
      wsUrl = 'wss://stream-demo.bybit.com/v5/public/linear';
      break;
    default:
      console.error(`❌ Unsupported exchange: ${exchange}`);
      return;
  }

  console.log(`🔌 Connecting to ${key} (1m candles)...`);
  const ws = new WebSocket(wsUrl);

  ws.on('open', () => {
    console.log(`✅ Connected: ${key}`);
    activeConnections.set(key, ws);
    
    // Bybit 구독 (1분봉)
    if (exchange.toLowerCase().includes('bybit')) {
      ws.send(JSON.stringify({
        op: 'subscribe',
        args: [`kline.1.${symbol}`]
      }));
      console.log(`📡 Bybit subscribed: kline.1.${symbol}`);
    }
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      // Binance 1분봉
      if (message.e === 'kline' && message.k && message.k.x) {
        const candle = {
          exchange,
          symbol,
          timeframe: '1m',
          timestamp: message.k.t,
          open: parseFloat(message.k.o),
          high: parseFloat(message.k.h),
          low: parseFloat(message.k.l),
          close: parseFloat(message.k.c),
          volume: parseFloat(message.k.v)
        };
        
        console.log(`📊 1m Candle: ${symbol} ${candle.close}`);
        
        // 모든 구독자에게 1분봉 그대로 전송
        for (const [subKey, users] of userSubscriptions.entries()) {
          if (subKey.startsWith(`${exchange}-${symbol}-`)) {
            for (const userId of users) {
              await sendToWorker(candle, userId);
            }
          }
        }
      }
      
      // Bybit 1분봉
      if (message.topic && message.topic.startsWith('kline') && message.data) {
        for (const kline of message.data) {
          if (!kline.confirm) continue;
          
          const candle = {
            exchange,
            symbol,
            timeframe: '1m',
            timestamp: kline.start,
            open: parseFloat(kline.open),
            high: parseFloat(kline.high),
            low: parseFloat(kline.low),
            close: parseFloat(kline.close),
            volume: parseFloat(kline.volume)
          };
          
          console.log(`📊 1m Candle: ${symbol} ${candle.close}`);
          
          for (const [subKey, users] of userSubscriptions.entries()) {
            if (subKey.startsWith(`${exchange}-${symbol}-`)) {
              for (const userId of users) {
                await sendToWorker(candle, userId);
              }
            }
          }
        }
      }
    } catch (error) {
      console.error(`⚠️ Message parse error:`, error.message);
    }
  });

  ws.on('close', () => {
    console.log(`❌ Disconnected: ${key}`);
    activeConnections.delete(key);
    
    // 재연결 (구독자가 있으면)
    setTimeout(() => {
      for (const [subKey, users] of userSubscriptions.entries()) {
        if (subKey.startsWith(`${exchange}-${symbol}-`) && users.size > 0) {
          console.log(`🔄 Reconnecting: ${key}`);
          connectExchange(exchange, symbol);
          break;
        }
      }
    }, 5000);
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error (${key}):`, error.message);
  });
}

// Worker로 1분봉 전송 (그대로!)
async function sendToWorker(candle, userId) {
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
      console.error(`❌ Worker response error:`, response.status);
    }
  } catch (error) {
    console.error(`❌ Send to worker failed:`, error.message);
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
  
  // WebSocket 연결 (항상 1분봉)
  const baseKey = `${exchange}-${symbol}`;
  if (!activeConnections.has(baseKey)) {
    connectExchange(exchange, symbol);
  }
  
  res.json({ 
    success: true, 
    message: `Connected to ${key} (receiving 1m candles)`,
    subscribers: userSubscriptions.get(key).size
  });
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
    
    // 더 이상 구독자가 없으면 정리
    if (userSubscriptions.get(key).size === 0) {
      userSubscriptions.delete(key);
      
      // 같은 exchange-symbol을 사용하는 다른 구독이 없으면 WebSocket 종료
      let hasOtherSubs = false;
      for (const [subKey] of userSubscriptions.entries()) {
        if (subKey.startsWith(`${exchange}-${symbol}-`)) {
          hasOtherSubs = true;
          break;
        }
      }
      
      if (!hasOtherSubs) {
        const baseKey = `${exchange}-${symbol}`;
        if (activeConnections.has(baseKey)) {
          activeConnections.get(baseKey).close();
          activeConnections.delete(baseKey);
          console.log(`🔌 WebSocket closed: ${baseKey} (no subscribers)`);
        }
      }
    }
  }
  
  res.json({ success: true, message: `Disconnected from ${key}` });
});

// Binance API 프록시
app.post('/proxy/binance', async (req, res) => {
  try {
    const { url, method, headers } = req.body;
    
    console.log('🔗 Proxying:', url);
    
    const response = await fetch(url, {
      method: method || 'GET',
      headers: headers || {}
    });
    
    const data = await response.text();
    
    console.log('📡 Proxy response:', response.status);
    
    res.status(response.status).send(data);
  } catch (error) {
    console.error('❌ Proxy error:', error);
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

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    connections: activeConnections.size,
    totalSubscribers: Array.from(userSubscriptions.values()).reduce((sum, set) => sum + set.size, 0),
    uptime: process.uptime()
  });
});

app.get('/', (req, res) => {
  res.send('CoinTop10 WebSocket Bridge - 1m Candles Only (Backtest Compatible)');
});

app.listen(PORT, () => {
  console.log(`🚀 Railway WebSocket Bridge running on port ${PORT}`);
  console.log('✅ Sending 1m candles only');
  console.log('✅ Worker will handle conversion');
  console.log('✅ Backtest compatible mode');
});
