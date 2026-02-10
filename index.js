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
const activeConnections = new Map(); // key: exchange-symbol-1m, value: WebSocket
const candleBuffers = new Map(); // key: userId, value: { candles: [], targetTimeframe: '15m' }

// 타임프레임을 분 단위로 변환
function timeframeToMinutes(timeframe) {
  const map = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '4h': 240 };
  return map[timeframe] || 1;
}

// 1분봉을 타겟 타임프레임으로 변환
function convertToTargetTimeframe(candles, targetTimeframe) {
  const minutes = timeframeToMinutes(targetTimeframe);
  
  if (candles.length < minutes) return null;
  
  // 마지막 N개 캔들 가져오기
  const chunk = candles.slice(-minutes);
  
  return {
    exchange: chunk[0].exchange,
    symbol: chunk[0].symbol,
    timeframe: targetTimeframe,
    timestamp: chunk[0].timestamp,
    open: chunk[0].open,
    high: Math.max(...chunk.map(c => c.high)),
    low: Math.min(...chunk.map(c => c.low)),
    close: chunk[chunk.length - 1].close,
    volume: chunk.reduce((sum, c) => sum + c.volume, 0)
  };
}

// WebSocket 연결 (항상 1분봉)
function connectExchange(exchange, symbol) {
  const key = `${exchange}-${symbol}-1m`;  // ✅ 항상 1분봉!
  
  if (activeConnections.has(key)) {
    console.log(`✅ Already connected: ${key}`);
    return;
  }

  let wsUrl;
  
  switch (exchange.toLowerCase()) {
    case 'binance':
      wsUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_1m`;  // ✅ 1m
      break;
    case 'binance-testnet':
      wsUrl = `wss://stream.binancefuture.com/ws/${symbol.toLowerCase()}@kline_1m`;  // ✅ 1m
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
    
    // Bybit 구독 (1분봉)
    if (exchange.toLowerCase().includes('bybit')) {
      ws.send(JSON.stringify({
        op: 'subscribe',
        args: [`kline.1.${symbol}`]  // ✅ 1분봉
      }));
      console.log(`📡 Bybit subscribed: kline.1.${symbol}`);
    }
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      // Binance 1분봉 캔들
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
        
        console.log(`📊 1m Candle (Binance): ${symbol} ${candle.close}`);
        
        // 모든 구독자의 버퍼에 추가
        for (const [subKey, users] of userSubscriptions.entries()) {
          if (subKey.startsWith(`${exchange}-${symbol}-`)) {
            const targetTimeframe = subKey.split('-')[2];
            
            for (const userId of users) {
              await processCandle(candle, userId, targetTimeframe);
            }
          }
        }
      }
      
      // Bybit 1분봉 캔들
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
          
          console.log(`📊 1m Candle (Bybit): ${symbol} ${candle.close}`);
          
          for (const [subKey, users] of userSubscriptions.entries()) {
            if (subKey.startsWith(`${exchange}-${symbol}-`)) {
              const targetTimeframe = subKey.split('-')[2];
              
              for (const userId of users) {
                await processCandle(candle, userId, targetTimeframe);
              }
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

// 1분봉 처리 및 변환
async function processCandle(candle1m, userId, targetTimeframe) {
  const bufferKey = `${userId}-${candle1m.exchange}-${candle1m.symbol}`;
  
  if (!candleBuffers.has(bufferKey)) {
    candleBuffers.set(bufferKey, { candles: [], targetTimeframe });
  }
  
  const buffer = candleBuffers.get(bufferKey);
  buffer.candles.push(candle1m);
  
  const requiredCandles = timeframeToMinutes(targetTimeframe);
  
  // 버퍼 크기 제한
  if (buffer.candles.length > requiredCandles * 2) {
    buffer.candles = buffer.candles.slice(-requiredCandles * 2);
  }
  
  // 타겟 타임프레임으로 변환
  if (buffer.candles.length >= requiredCandles) {
    const convertedCandle = convertToTargetTimeframe(buffer.candles, targetTimeframe);
    
    if (convertedCandle) {
      console.log(`🔄 Converted to ${targetTimeframe}: ${convertedCandle.symbol} ${convertedCandle.close}`);
      await sendToWorker(convertedCandle, userId);
      
      // 변환 완료 후 버퍼에서 사용한 캔들 제거
      buffer.candles = buffer.candles.slice(requiredCandles);
    }
  }
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
  
  // WebSocket 연결 (항상 1분봉으로!)
  const baseKey = `${exchange}-${symbol}-1m`;
  if (!activeConnections.has(baseKey)) {
    connectExchange(exchange, symbol);
  }
  
  res.json({ 
    success: true, 
    message: `Connected to ${key} (via 1m candles)`,
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
        const baseKey = `${exchange}-${symbol}-1m`;
        if (activeConnections.has(baseKey)) {
          activeConnections.get(baseKey).close();
          activeConnections.delete(baseKey);
          console.log(`🔌 WebSocket closed: ${baseKey} (no subscribers)`);
        }
      }
    }
  }
  
  // 버퍼 정리
  const bufferKey = `${userId}-${exchange}-${symbol}`;
  candleBuffers.delete(bufferKey);
  
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
    userSubscriptions: {},
    candleBuffers: Array.from(candleBuffers.keys())
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
    buffers: candleBuffers.size,
    uptime: process.uptime()
  });
});

app.get('/', (req, res) => {
  res.send('CoinTop10 WebSocket Bridge - 1m to Any Timeframe (Backtest Compatible)');
});

app.listen(PORT, () => {
  console.log(`🚀 Railway WebSocket Bridge running on port ${PORT}`);
  console.log('✅ Multi-user support enabled');
  console.log('✅ 1m candles → Any timeframe conversion');
  console.log('✅ Backtest compatible mode');
});
