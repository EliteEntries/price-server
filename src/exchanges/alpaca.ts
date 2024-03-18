import Alpaca from '@alpacahq/alpaca-trade-api'
import redis, { RedisClientType, createClient } from 'redis'
import WebSocket from 'ws'
import 'dotenv/config'

const Publisher: RedisClientType = createClient({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
})
Publisher.on('error', (err) => console.log('Redis Publisher Error', err))

let candleTimers: any = {}
let tradeTimers: any = {}
let lastTrades: any = {}
let Subscriptions: any[] = []

const alpaca = new Alpaca({
    keyId: process.env.ALPACA_KEY,
    secretKey: process.env.ALPACA_SECRET,
    paper: true,
})

let server: WebSocket

async function Socket() {
    await Publisher.connect()
    const result = await Publisher.get('exchanges:alpaca:tracked')
    server = new WebSocket('wss://stream.data.alpaca.markets/v2/iex')

    candleTimers = {}, tradeTimers = {}
    
    server.addEventListener('error', async function(e) {
        await Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions))
        server.close()
        setTimeout(Socket, 5000)
        process.send && process.send(['alpaca', 'error'])
    })

    server.addEventListener('close', async function(e) {
        await Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions))
        server.close()
        setTimeout(Socket, 5000)
        process.send && process.send(['alpaca', 'lost-connection'])
    })

    server.addEventListener('open', async function(e) {
        process.send && process.send(['alpaca', 'open'])
        server.on('message', async (resp: string) => {
            const msgs: string[] = JSON.parse(resp)
            msgs.forEach( async (msg: any) => {
                const message = msg.msg
                const status = msg.T

                if (status == 'success' && message) 
                    switch (message) {
                        case 'connected': {
                            server.send(JSON.stringify({
                                action : 'auth',
                                key : process.env.ALPACA_KEY,
                                secret : process.env.ALPACA_SECRET
                            }))
                            break;
                        }
                        case 'authenticated' : {            
                            if (result) {
                                let trades: string[] = [], candles: string[] = []
                                JSON.parse(result).forEach( (subscription: string[]) =>{ 
                                    subscription[0] == 'sub-trades' && trades.push(subscription[1])
                                    subscription[0] == 'sub-candles' && candles.push(subscription[1])
                                })
                                server.send(JSON.stringify({
                                    action : 'subscribe',
                                    trades : trades,
                                    bars : candles
                                }))
                            }
                            break;
                        }
                        default : {
                            console.log('----', message)
                            break;
                        }
                    }
                else switch (status) {
                    case 't' : {
                        await Publisher.set('price:alpaca:'+msg.S, msg.p)
                        console.log('')
                        break;
                    }
                    case 'subscription' : {
                        msg.trades.length > 0 && console.log(`Subscribed to ${msg.trades} trades`)
                        msg.bars.length > 0 && console.log(`Subscribed to ${msg.bars} candles`)
                        break;
                    }
                    default : {
                        console.log(msg)
                        break;
                    }
                }
            })
        })
    })
}

async function subscribeCandles(market: string, timeframe: string, first?: boolean) {
    try {
       // let candles = await getCandles(market, timeframe, first ? 200 : 1)
       console.log('candles')
    } catch(e) {
        setTimeout(()=>subscribeCandles(market, timeframe, false), 1000)
    }
}


async function executeMessage(message: string[]) {
    switch (message[0]) {
        case 'sub-trades' : {
            if (!Subscriptions.toString().includes(message.toString())) {
                server?.send(JSON.stringify({
                    action : 'subscribe',
                    trades : [message[1].toUpperCase()]
                }))
                process.send && process.send(['alpaca', 'sub-trades', message[1].toUpperCase()])
                Subscriptions.push(message) 
                Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions))
            }
            break;
        }
        case 'unsub-trades' : {
            if (Subscriptions.filter((sub) => { return (sub[0] == 'sub-trades' && sub[1] == message[1])}).length == 1) {
                server?.send(JSON.stringify({
                    action : 'unsubscribe',
                    trades : [message[1].toUpperCase()],
                }))
                
                Publisher.del(`${message[1].toUpperCase()}-alpaca`)
                Subscriptions.forEach((sub, index)=>{
                    if (sub[0] == 'sub-trades' && sub[1] == message[1]) {
                        Subscriptions.splice(index,1)
                        Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions))
                    }
                })
            }
            break;
        }
    }
}


process.on('message', (message: string[]) => {
    executeMessage(message)
})



const equities = alpaca.data_stream_v2;
const crypto = alpaca.crypto_stream_v2;
const news = alpaca.news_stream;
crypto.onConnect(async () => {
    console.log('crypto connected')
})
crypto.onCryptoTrade((trade) => {
    console.log('Crypto Trade:', trade);
})
crypto.onError((err) => {
    console.log("Crypto Error:", err);
})
equities.onConnect(async () => {
    process.send && process.send(['alpaca', 'open'])
    console.log('open')
});
equities.onStateChange(async (status) => { 
  console.log("Status:", status);
  
  switch (status) {
    case 'authenticated' : {
        await Publisher.connect()
        const result = await Publisher.get('exchanges:alpaca:tracked') 
        if (result) {
            let trades: string[] = [], candles: string[] = []
            JSON.parse(result).forEach( (subscription: string[]) => { 
                subscription[0] == 'sub-trades' && trades.push(subscription[1])
                subscription[0] == 'sub-candles' && candles.push(subscription[1])
            })
            equities.subscribeForTrades(trades)
            equities.subscribeForBars(candles)
        }
        break;
    }
  }
});
equities.onError((err) => {
  console.log("Error:", err);
});

equities.onStockTrade((trade) => {
  console.log(`${trade.Size} shares ${trade.Symbol} @ ${trade.Price}`)
});

equities.onStockBar((bar) => {
    console.log("Bar:", bar)
});

equities.onStatuses((statuses) => {
    console.log("Statuses:", statuses);
})
equities.addListener('message', (msg)=>{
    console.log(msg)
})
equities.connect();