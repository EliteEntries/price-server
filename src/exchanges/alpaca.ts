import Alpaca from '@alpacahq/alpaca-trade-api'
import redis, { RedisClientType, createClient } from 'redis'
import https from 'https'
import 'dotenv/config'

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
    if (!Publisher.isOpen) await Publisher.connect();
    const result = await Publisher.get('exchanges:alpaca:tracked')
    if (result) {
        JSON.parse(result).forEach( (subscription: string[]) => { 
            subscription[0] == 'stock-trades' && Stocks.push(subscription[1])
            subscription[0] == 'crypto-trades' && Cryptos.push(subscription[1])
            subscription[0] == 'option-prices' && Options.push(subscription[1])
            subscription[0] == 'sub-candles' && Candles.push(subscription[1])
        })

    }

    setInterval( async ()=>{
        if (Stocks.length && Stocks.length > 0) await getStockPrices(Stocks)
        if (Cryptos.length && Cryptos.length > 0) await getCryptoPrices(Cryptos)
        if (Options.length && Options.length > 0) await getOptionsPrices(Options)
    }, 1000)

}
main()

export function getOptionsPrices(data: string|string[]) {
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

export async function getStockPrices(symbols: string|string[]) {
    if (!Array.isArray(symbols)) symbols = [symbols]
    const resp = await alpaca.getLatestTrades(symbols)
    resp.forEach((trade: any) => {
        Publisher.set('price:alpaca:'+trade.Symbol, trade.Price)
    })
}

export async function getCryptoPrices(data: string|string[]) {
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