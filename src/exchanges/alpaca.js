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
const alpaca_trade_api_1 = __importDefault(require("@alpacahq/alpaca-trade-api"));
const redis_1 = require("redis");
const ws_1 = __importDefault(require("ws"));
require("dotenv/config");
const Publisher = (0, redis_1.createClient)({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
});
Publisher.on('error', (err) => console.log('Redis Publisher Error', err));
let candleTimers = {};
let tradeTimers = {};
let lastTrades = {};
let Subscriptions = [];
const alpaca = new alpaca_trade_api_1.default({
    keyId: process.env.ALPACA_KEY,
    secretKey: process.env.ALPACA_SECRET,
    paper: true,
});
let server;
function Socket() {
    return __awaiter(this, void 0, void 0, function* () {
        yield Publisher.connect();
        const result = yield Publisher.get('exchanges:alpaca:tracked');
        server = new ws_1.default('wss://stream.data.alpaca.markets/v2/iex');
        candleTimers = {}, tradeTimers = {};
        server.addEventListener('error', function (e) {
            return __awaiter(this, void 0, void 0, function* () {
                yield Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions));
                server.close();
                setTimeout(Socket, 5000);
                process.send && process.send(['alpaca', 'error']);
            });
        });
        server.addEventListener('close', function (e) {
            return __awaiter(this, void 0, void 0, function* () {
                yield Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions));
                server.close();
                setTimeout(Socket, 5000);
                process.send && process.send(['alpaca', 'lost-connection']);
            });
        });
        server.addEventListener('open', function (e) {
            return __awaiter(this, void 0, void 0, function* () {
                process.send && process.send(['alpaca', 'open']);
                server.on('message', (resp) => __awaiter(this, void 0, void 0, function* () {
                    const msgs = JSON.parse(resp);
                    msgs.forEach((msg) => __awaiter(this, void 0, void 0, function* () {
                        const message = msg.msg;
                        const status = msg.T;
                        if (status == 'success' && message)
                            switch (message) {
                                case 'connected': {
                                    server.send(JSON.stringify({
                                        action: 'auth',
                                        key: process.env.ALPACA_KEY,
                                        secret: process.env.ALPACA_SECRET
                                    }));
                                    break;
                                }
                                case 'authenticated': {
                                    if (result) {
                                        let trades = [], candles = [];
                                        JSON.parse(result).forEach((subscription) => {
                                            subscription[0] == 'sub-trades' && trades.push(subscription[1]);
                                            subscription[0] == 'sub-candles' && candles.push(subscription[1]);
                                        });
                                        server.send(JSON.stringify({
                                            action: 'subscribe',
                                            trades: trades,
                                            bars: candles
                                        }));
                                    }
                                    break;
                                }
                                default: {
                                    console.log('----', message);
                                    break;
                                }
                            }
                        else
                            switch (status) {
                                case 't': {
                                    yield Publisher.set('price:alpaca:' + msg.S, msg.p);
                                    console.log('');
                                    break;
                                }
                                case 'subscription': {
                                    msg.trades.length > 0 && console.log(`Subscribed to ${msg.trades} trades`);
                                    msg.bars.length > 0 && console.log(`Subscribed to ${msg.bars} candles`);
                                    break;
                                }
                                default: {
                                    console.log(msg);
                                    break;
                                }
                            }
                    }));
                }));
            });
        });
    });
}
function subscribeCandles(market, timeframe, first) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            // let candles = await getCandles(market, timeframe, first ? 200 : 1)
            console.log('candles');
        }
        catch (e) {
            setTimeout(() => subscribeCandles(market, timeframe, false), 1000);
        }
    });
}
function executeMessage(message) {
    return __awaiter(this, void 0, void 0, function* () {
        switch (message[0]) {
            case 'sub-trades': {
                if (!Subscriptions.toString().includes(message.toString())) {
                    server === null || server === void 0 ? void 0 : server.send(JSON.stringify({
                        action: 'subscribe',
                        trades: [message[1].toUpperCase()]
                    }));
                    process.send && process.send(['alpaca', 'sub-trades', message[1].toUpperCase()]);
                    Subscriptions.push(message);
                    Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions));
                }
                break;
            }
            case 'unsub-trades': {
                if (Subscriptions.filter((sub) => { return (sub[0] == 'sub-trades' && sub[1] == message[1]); }).length == 1) {
                    server === null || server === void 0 ? void 0 : server.send(JSON.stringify({
                        action: 'unsubscribe',
                        trades: [message[1].toUpperCase()],
                    }));
                    Publisher.del(`${message[1].toUpperCase()}-alpaca`);
                    Subscriptions.forEach((sub, index) => {
                        if (sub[0] == 'sub-trades' && sub[1] == message[1]) {
                            Subscriptions.splice(index, 1);
                            Publisher.set('exchanges:alpaca:tracked', JSON.stringify(Subscriptions));
                        }
                    });
                }
                break;
            }
        }
    });
}
process.on('message', (message) => {
    executeMessage(message);
});
const equities = alpaca.data_stream_v2;
const crypto = alpaca.crypto_stream_v2;
const news = alpaca.news_stream;
crypto.onConnect(() => __awaiter(void 0, void 0, void 0, function* () {
    console.log('crypto connected');
}));
crypto.onCryptoTrade((trade) => {
    console.log('Crypto Trade:', trade);
});
crypto.onError((err) => {
    console.log("Crypto Error:", err);
});
equities.onConnect(() => __awaiter(void 0, void 0, void 0, function* () {
    process.send && process.send(['alpaca', 'open']);
    console.log('open');
}));
equities.onStateChange((status) => __awaiter(void 0, void 0, void 0, function* () {
    console.log("Status:", status);
    switch (status) {
        case 'authenticated': {
            yield Publisher.connect();
            const result = yield Publisher.get('exchanges:alpaca:tracked');
            if (result) {
                let trades = [], candles = [];
                JSON.parse(result).forEach((subscription) => {
                    subscription[0] == 'sub-trades' && trades.push(subscription[1]);
                    subscription[0] == 'sub-candles' && candles.push(subscription[1]);
                });
                equities.subscribeForTrades(trades);
                equities.subscribeForBars(candles);
            }
            break;
        }
    }
}));
equities.onError((err) => {
    console.log("Error:", err);
});
equities.onStockTrade((trade) => {
    console.log(`${trade.Size} shares ${trade.Symbol} @ ${trade.Price}`);
});
equities.onStockBar((bar) => {
    console.log("Bar:", bar);
});
equities.onStatuses((statuses) => {
    console.log("Statuses:", statuses);
});
equities.addListener('message', (msg) => {
    console.log(msg);
});
equities.connect();
