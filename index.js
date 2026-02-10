const fetch = require('node-fetch');
const WebSocket = require('ws');
const express = require('express');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const WORKER_URL = process.env.WORKER_URL || 'https://cointop10-forward.cointop10-com.workers.dev';

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    connections: connections.size,
    uptime: process.uptime()
  });
});

const connections = new Map();

function connectExchange(exchange, symbol, timeframe) {
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  if (connections.has(key)) {
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
    
    if (exchange.toLowerCase().includes('bybit')) {
      ws.send(JSON.stringify({
        op: 'subscribe',
        args: [`kline.${timeframe}.${symbol}`]
      }));
    }
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
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
        
        await sendToWorker(candle);
      }
      
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
          
          await sendToWorker(candle);
        }
      }
    } catch (error) {
      console.error(`⚠️ Message parse error (${key}):`, error.message);
    }
  });

  ws.on('close', () => {
    console.log(`❌ Disconnected: ${key}`);
    connections.delete(key);
    
    setTimeout(() => {
      console.log(`🔄 Reconnecting: ${key}`);
      connectExchange(exchange, symbol, timeframe);
    }, 5000);
  });

  ws.on('error', (error) => {
    console.error(`❌ WebSocket error (${key}):`, error.message);
  });

  connections.set(key, ws);
}

async function sendToWorker(candle) {
  try {
    const response = await fetch(`${WORKER_URL}/api/new-candle`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(candle)
    });

    if (!response.ok) {
      throw new Error(`Worker responded with ${response.status}`);
    }

    console.log(`📊 Candle sent: ${candle.exchange} ${candle.symbol} ${candle.close}`);
  } catch (error) {
    console.error('⚠️ Failed to send to Worker:', error.message);
  }
}

app.post('/connect', (req, res) => {
  const { exchange, symbol, timeframe } = req.body;
  
  if (!exchange || !symbol || !timeframe) {
    return res.status(400).json({ 
      error: 'Missing required fields: exchange, symbol, timeframe' 
    });
  }

  connectExchange(exchange, symbol, timeframe);
  
  res.json({ 
    success: true, 
    message: `Connected to ${exchange} ${symbol} ${timeframe}`,
    totalConnections: connections.size
  });
});

app.post('/disconnect', (req, res) => {
  const { exchange, symbol, timeframe } = req.body;
  const key = `${exchange}-${symbol}-${timeframe}`;
  
  const ws = connections.get(key);
  if (ws) {
    ws.close();
    connections.delete(key);
    res.json({ success: true, message: `Disconnected ${key}` });
  } else {
    res.status(404).json({ error: `Connection not found: ${key}` });
  }
});

app.get('/connections', (req, res) => {
  const list = Array.from(connections.keys());
  res.json({ 
    total: list.length,
    connections: list
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Railway WebSocket Bridge running on port ${PORT}`);
  console.log(`📡 Worker URL: ${WORKER_URL}`);
});

// Binance API 프록시
app.post('/proxy/binance', express.json(), async (req, res) => {
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

console.log('✅ Binance proxy endpoint added');
