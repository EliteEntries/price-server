import Alpaca from '@alpacahq/alpaca-trade-api'
import redis, { RedisClientType, createClient } from 'redis'
import https from 'https'
import { initializeApp, credential } from 'firebase-admin'
import { getFirestore } from 'firebase-admin/firestore'

import serviceAccount from '../../util/firebase.json'
import 'dotenv/config'

initializeApp( { credential: credential.cert(serviceAccount as any) } )
const db = getFirestore()

const Publisher: RedisClientType = createClient({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
})

Publisher.on('error', (err) => console.log('Redis Publisher Error', err))

const alpaca = new Alpaca({
    keyId: process.env.ALPACA_KEY,
    secretKey: process.env.ALPACA_SECRET,
    paper: true,
})

let Stocks: any[] = [], Cryptos:any[] = [], Options: any[] = [],
    Candles: any[] = []

async function main() {
    if (!Publisher.isOpen) await Publisher.connect()
    const snapshot = await db.collection('exchanges').doc('alpaca').get()

    const result = await Publisher.get('exchanges:alpaca:tracked')

    if (result) {
        JSON.parse(result).forEach( (subscription: string[]) => { 
            subscription[0] == 'stock-trades' && Stocks.push(subscription[1])
            subscription[0] == 'crypto-trades' && Cryptos.push(subscription[1])
            subscription[0] == 'option-prices' && Options.push(subscription[1])
            subscription[0] == 'sub-candles' && Candles.push(subscription[1])
        })
    }

    if (snapshot.exists) {
        const data = snapshot.data()
        if (data?.tracked) {
            data.tracked.forEach( (subscription: string[]) => { 
                subscription[0] == 'stock-trades' && Stocks.push(subscription[1])
                subscription[0] == 'crypto-trades' && Cryptos.push(subscription[1])
                subscription[0] == 'option-prices' && Options.push(subscription[1])
                subscription[0] == 'sub-candles' && Candles.push(subscription[1])
            })
        }
    }

    setInterval( async ()=>{
        if (Stocks.length && Stocks.length > 0) await getStockPrices(Stocks)
        if (Cryptos.length && Cryptos.length > 0) await getCryptoPrices(Cryptos)
        if (Options.length && Options.length > 0) await getOptionsPrices(Options)
    }, 1000)

    //! Add a listener for new subscriptions and unsubscriptions on firestore db
    db.collection('exchanges').doc('alpaca').onSnapshot( async (snapshot) => {
        const data = snapshot.data()
        if (data?.tracked) {
            data.tracked.forEach( async (subscription: string[]) => {
                if (subscription[0] == 'stock-trades' && !Stocks.includes(subscription[1])) {
                    await addTrack(subscription[1], subscription[0])
                } else if (subscription[0] == 'crypto-trades' && !Cryptos.includes(subscription[1])) {
                    await addTrack(subscription[1], subscription[0])
                } else if (subscription[0] == 'option-prices' && !Options.includes(subscription[1])) {
                    await addTrack(subscription[1], subscription[0])
                } else if (subscription[0] == 'sub-candles' && !Candles.includes(subscription[1])) {
                    await addTrack(subscription[1], subscription[0])
                }
            })
        }
    })
    

}

main()

async function getOptionsPrices(data: string|string[]) {
    const symbols = Array.isArray(data) ? data.join(',') : data
    const opts = {
        method: 'GET',
        headers: {
          accept: 'application/json',
          'APCA-API-KEY-ID': process.env.ALPACA_KEY,
          'APCA-API-SECRET-KEY': process.env.ALPACA_SECRET
        }
      };
    
    https.get(`https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols=${symbols}&feed=indicative`, opts,
        (res) => {
            let data: any = ''
            res.on('data', (chunk) => {
                data += chunk
            })
            res.on('end', () => {
                data = JSON.parse(data)
                Object.keys(data.quotes).forEach(async (symbol: string) => {
                    const bid = data.quotes[symbol].bp
                    const ask = data.quotes[symbol].ap
                    const bidSize = data.quotes[symbol].bs
                    const askSize = data.quotes[symbol].as
                    await Publisher.set('bid:alpaca:'+symbol, bid)
                    await Publisher.set('ask:alpaca:'+symbol, ask)
                })
            })
            res.on('error', (err) => {
                console.log(err)
            })
        })
}

async function getStockPrices(symbols: string|string[]) {
    if (!Array.isArray(symbols)) symbols = [symbols]
    const resp = await alpaca.getLatestTrades(symbols)
    resp.forEach((trade: any) => {
        Publisher.set('price:alpaca:'+trade.Symbol, trade.Price)
    })
}

async function getCryptoPrices(data: string|string[]) {
    const symbols: string = Array.isArray(data) ? data.join(',') : data
    symbols.replace('/','%2F')
    const opts = {
        method: 'GET',
        headers: {
            accept: 'application/json',
            'APCA-API-KEY-ID': process.env.ALPACA_KEY,
            'APCA-API-SECRET-KEY': process.env.ALPACA_SECRET
        }
    };
    https.get(`https://data.alpaca.markets/v1beta3/crypto/us/latest/trades?symbols=${symbols}`, opts, (res) => {
        let data: any = ''
        res.on('data', (chunk) => {
            data += chunk
        })
        res.on('end', () => {
            data = JSON.parse(data)
            Object.keys(data.trades).forEach((symbol: string) => {
                const trade = data.trades[symbol]
                if (Date.now() - (new Date(trade.t)).getTime() > (60000*60)) return
                Publisher.set('price:alpaca:'+symbol, trade.p)
            })
        })
        res.on('error', (err) => {
            console.log(err)
        })
    
    })

}

async function addTrack(symbol: string, type: string) {
    const data = await Publisher.get('exchanges:alpaca:tracked')
    const tracked: any[] = data ? JSON.parse(data) : []
    tracked.push([type, symbol])
    await Publisher.set('exchanges:alpaca:tracked', JSON.stringify(tracked))
    await db.collection('exchanges').doc('alpaca').set({ tracked: tracked }, { merge: true })
    switch (type) {
        case 'stock-trades':
            Stocks.push(symbol)
            break;
        case 'crypto-trades':
            Cryptos.push(symbol)
            break;
        case 'option-prices':
            Options.push(symbol)
            break;
        case 'sub-candles':
            Candles.push(symbol)
            break;
        default:
            break;
    }
}

async function removeTrack(symbol: string, type: string) {
    const data = await Publisher.get('exchanges:alpaca:tracked')
    const tracked: any[] = data ? JSON.parse(data) : []
    const index = tracked.findIndex((track: string[]) => track[0] == type && track[1] == symbol)
    if (index > -1) {
        tracked.splice(index, 1)
        await Publisher.set('exchanges:alpaca:tracked', JSON.stringify(tracked))
        await db.collection('exchanges').doc('alpaca').set({ tracked: tracked }, { merge: true })
    }
    switch (type) {
        case 'stock-trades':
            Stocks = Stocks.filter((stock: string) => stock != symbol)
            await Publisher.del('price:alpaca:'+symbol)
            break;
        case 'crypto-trades':
            Cryptos = Cryptos.filter((crypto: string) => crypto != symbol)
            await Publisher.del('price:alpaca:'+symbol)
            break;
        case 'option-prices':
            Options = Options.filter((option: string) => option != symbol)
            await Publisher.del('bid:alpaca:'+symbol)
            await Publisher.del('ask:alpaca:'+symbol)
            break;
        case 'sub-candles':
            Candles = Candles.filter((candle: string) => candle != symbol)
            break;
        default:
            break;
    }
}