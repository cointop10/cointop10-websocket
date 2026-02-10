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
const activeConnections = new Map(); // key: exchange-symbol-timeframe, value: WebSocket

// WebSocket 연결
function connectExchange(exchange, symbol, timeframe) {
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  if (activeConnections.has(key)) {
    console.log(`✅ Already connected: ${key}`);
    return;
  }

  let wsUrl;
  
  switch (exchange.toLowerCase()) {
    case 'binance':
      wsUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_${timeframe}`;
      break;
    case 'binance-testnet':
      wsUrl = `wss://stream.binancefuture.com/ws/${symbol.toLowerCase()}@kline_${timeframe}`;
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

  console.log(`🔌 Connecting to ${key}...`);
  const ws = new WebSocket(wsUrl);

  ws.on('open', () => {
    console.log(`✅ Connected: ${key}`);
    activeConnections.set(key, ws);
    
    // Bybit 구독
    if (exchange.toLowerCase().includes('bybit')) {
      ws.send(JSON.stringify({
        op: 'subscribe',
        args: [`kline.${timeframe}.${symbol}`]
      }));
      console.log(`📡 Bybit subscribed: kline.${timeframe}.${symbol}`);
    }
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      // Binance 캔들
      if (message.e === 'kline' && message.k && message.k.x) {
        const candle = {
          exchange,
          symbol,
          timeframe,
          timestamp: message.k.t,
          open: parseFloat(message.k.o),
          high: parseFloat(message.k.h),
          low: parseFloat(message.k.l),
          close: parseFloat(message.k.c),
          volume: parseFloat(message.k.v)
        };
        
        console.log(`📊 Candle (Binance): ${key} ${candle.close}`);
        
        // 이 구독을 가진 모든 사용자에게 전송
        const users = userSubscriptions.get(key);
        if (users && users.size > 0) {
          for (const userId of users) {
            await sendToWorker(candle, userId);
          }
        }
      }
      
      // Bybit 캔들
      if (message.topic && message.topic.startsWith('kline') && message.data) {
        for (const kline of message.data) {
          if (!kline.confirm) continue;
          
          const candle = {
            exchange,
            symbol,
            timeframe,
            timestamp: kline.start,
            open: parseFloat(kline.open),
            high: parseFloat(kline.high),
            low: parseFloat(kline.low),
            close: parseFloat(kline.close),
            volume: parseFloat(kline.volume)
          };
          
          console.log(`📊 Candle (Bybit): ${key} ${candle.close}`);
          
          const users = userSubscriptions.get(key);
          if (users && users.size > 0) {
            for (const userId of users) {
              await sendToWorker(candle, userId);
            }
          }
        }
      }
    } catch (error) {
      console.error(`⚠️ Message parse error (${key}):`, error.message);
    }
  });

  ws.on('close', () => {
    console.log(`❌ Disconnected: ${key}`);
    activeConnections.delete(key);
    
    // 재연결 (구독자가 있으면)
    setTimeout(() => {
      const users = userSubscriptions.get(key);
      if (users && users.size > 0) {
        console.log(`🔄 Reconnecting: ${key}`);
        connectExchange(exchange, symbol, timeframe);
      }
    }, 5000);
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error (${key}):`, error.message);
  });
}

// Worker로 캔들 전송
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
    connectExchange(exchange, symbol, timeframe);
  }
  
  res.json({ 
    success: true, 
    message: `Connected to ${key}`,
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
    
    // 더 이상 구독자가 없으면 WebSocket 종료
    if (userSubscriptions.get(key).size === 0) {
      userSubscriptions.delete(key);
      
      if (activeConnections.has(key)) {
        activeConnections.get(key).close();
        activeConnections.delete(key);
        console.log(`🔌 WebSocket closed: ${key} (no subscribers)`);
      }
    } else {
      console.log(`📊 Remaining subscribers for ${key}: ${userSubscriptions.get(key).size}`);
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
  res.send('CoinTop10 WebSocket Bridge - Multi-User Support (Binance + Bybit)');
});

app.listen(PORT, () => {
  console.log(`🚀 Railway WebSocket Bridge running on port ${PORT}`);
  console.log('✅ Multi-user support enabled');
  console.log('✅ Binance & Bybit supported');
});
