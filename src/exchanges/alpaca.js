"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getCryptoPrices = exports.getStockPrices = exports.getOptionsPrices = void 0;
const alpaca_trade_api_1 = __importDefault(require("@alpacahq/alpaca-trade-api"));
const redis_1 = require("redis");
const https_1 = __importDefault(require("https"));
require("dotenv/config");
const Publisher = (0, redis_1.createClient)({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
});
Publisher.on('error', (err) => console.log('Redis Publisher Error', err));
const alpaca = new alpaca_trade_api_1.default({
    keyId: process.env.ALPACA_KEY,
    secretKey: process.env.ALPACA_SECRET,
    paper: true,
});
let Stocks = [], Cryptos = [], Options = [], Candles = [];
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        if (!Publisher.isOpen)
            yield Publisher.connect();
        const result = yield Publisher.get('exchanges:alpaca:tracked');
        if (result) {
            JSON.parse(result).forEach((subscription) => {
                subscription[0] == 'stock-trades' && Stocks.push(subscription[1]);
                subscription[0] == 'crypto-trades' && Cryptos.push(subscription[1]);
                subscription[0] == 'option-prices' && Options.push(subscription[1]);
                subscription[0] == 'sub-candles' && Candles.push(subscription[1]);
            });
        }
        setInterval(() => __awaiter(this, void 0, void 0, function* () {
            if (Stocks.length && Stocks.length > 0)
                yield getStockPrices(Stocks);
            if (Cryptos.length && Cryptos.length > 0)
                yield getCryptoPrices(Cryptos);
            if (Options.length && Options.length > 0)
                yield getOptionsPrices(Options);
        }), 1000);
    });
}
main();
function getOptionsPrices(data) {
    const symbols = Array.isArray(data) ? data.join(',') : data;
    const opts = {
        method: 'GET',
        headers: {
            accept: 'application/json',
            'APCA-API-KEY-ID': process.env.ALPACA_KEY,
            'APCA-API-SECRET-KEY': process.env.ALPACA_SECRET
        }
    };
    https_1.default.get(`https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols=${symbols}&feed=indicative`, opts, (res) => {
        let data = '';
        res.on('data', (chunk) => {
            data += chunk;
        });
        res.on('end', () => {
            data = JSON.parse(data);
            Object.keys(data.quotes).forEach((symbol) => __awaiter(this, void 0, void 0, function* () {
                const bid = data.quotes[symbol].bp;
                const ask = data.quotes[symbol].ap;
                const bidSize = data.quotes[symbol].bs;
                const askSize = data.quotes[symbol].as;
                yield Publisher.set('bid:alpaca:' + symbol, bid);
                yield Publisher.set('ask:alpaca:' + symbol, ask);
            }));
        });
        res.on('error', (err) => {
            console.log(err);
        });
    });
}
exports.getOptionsPrices = getOptionsPrices;
function getStockPrices(symbols) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!Array.isArray(symbols))
            symbols = [symbols];
        const resp = yield alpaca.getLatestTrades(symbols);
        resp.forEach((trade) => {
            Publisher.set('price:alpaca:' + trade.Symbol, trade.Price);
        });
    });
}
exports.getStockPrices = getStockPrices;
function getCryptoPrices(data) {
    return __awaiter(this, void 0, void 0, function* () {
        const symbols = Array.isArray(data) ? data.join(',') : data;
        symbols.replace('/', '%2F');
        const opts = {
            method: 'GET',
            headers: {
                accept: 'application/json',
                'APCA-API-KEY-ID': process.env.ALPACA_KEY,
                'APCA-API-SECRET-KEY': process.env.ALPACA_SECRET
            }
        };
        https_1.default.get(`https://data.alpaca.markets/v1beta3/crypto/us/latest/trades?symbols=${symbols}`, opts, (res) => {
            let data = '';
            res.on('data', (chunk) => {
                data += chunk;
            });
            res.on('end', () => {
                data = JSON.parse(data);
                Object.keys(data.trades).forEach((symbol) => {
                    const trade = data.trades[symbol];
                    if (Date.now() - (new Date(trade.t)).getTime() > (60000 * 60))
                        return;
                    Publisher.set('price:alpaca:' + symbol, trade.p);
                });
            });
            res.on('error', (err) => {
                console.log(err);
            });
        });
    });
}
exports.getCryptoPrices = getCryptoPrices;
