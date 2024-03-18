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
Object.defineProperty(exports, "__esModule", { value: true });
const child_process_1 = require("child_process");
const redis_1 = require("redis");
require("dotenv/config");
const exchanges = [];
console.log(`redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`);
const Publisher = (0, redis_1.createClient)({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
});
let Listener = Publisher.duplicate();
Listener.on('error', (err) => console.log('Redis Listener Error', err));
Publisher.on('error', (err) => console.log('Redis Publisher Error', err));
console.clear();
const closeExchange = (exchange) => {
    for (let i = 0; i < exchanges.length; i++) {
        if (exchanges[i][0] == exchange) {
            exchanges[i][1].kill();
            exchanges.splice(i, 1);
        }
    }
};
const openExchange = (exchange) => {
    let there = false;
    for (let i = 0; i < exchanges.length; i++) {
        if (exchanges[i][0] == exchange) {
            there = true;
        }
    }
    if (!there) {
        const exchangeProcess = (0, child_process_1.fork)(`./exchanges/${exchange}.js`);
        exchanges.push([exchange, exchangeProcess]);
        exchangeProcess.on('exit', () => {
            console.log(`Exchange ${exchange} closed`);
            closeExchange(exchange);
            setTimeout(() => openExchange(exchange), 5000);
        });
        exchangeProcess.on('message', (message) => {
            switch (message[1]) {
                case 'lost-connection': {
                    console.log('Lost connection to ' + message[0] + ', reconnecting in 5 seconds.');
                    break;
                }
                case 'open': {
                    console.log(message[0] + ' connected successfully.');
                    break;
                }
                case 'error': {
                    console.log(message[0] + ' price error, reconnecting in 5 seconds.');
                    break;
                }
                case 'sub-trades': {
                    console.log("Subscribed to " + message[2] + ' trades. ' + exchange);
                    break;
                }
                case 'unsub-trades': {
                    console.log("Unsubscribed to " + message[2] + ' trades. ' + exchange);
                    break;
                }
                case 'sub-candles': {
                    console.log("Subscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange);
                    break;
                }
                case 'already-subscribed': {
                    console.log("Already subscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange);
                    break;
                }
                case 'unsub-candles': {
                    console.log("Unsubscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange);
                    break;
                }
            }
        });
    }
};
(() => __awaiter(void 0, void 0, void 0, function* () {
    yield Publisher.connect();
    yield Listener.connect();
    yield Listener.subscribe('Price Server', (message) => {
        message = JSON.parse(message);
        switch (message[0]) {
            case 'start': {
                openExchange(message[1]);
                break;
            }
            case 'close': {
                closeExchange(message[1]);
                break;
            }
            case 'restart': {
                closeExchange(message[1]);
                openExchange(message[1]);
                break;
            }
            case 'sub-trades':
            case 'unsub-trades': {
                for (let i = 0; i < exchanges.length; i++) {
                    if (exchanges[i][0] == message[1]) {
                        exchanges[i][1].send([message[0], message[2]]);
                    }
                }
                break;
            }
            case 'sub-candles':
            case 'unsub-candles': {
                for (let i = 0; i < exchanges.length; i++) {
                    if (exchanges[i][0] == message[1]) {
                        exchanges[i][1].send([message[0], message[2], message[3]]);
                    }
                }
                break;
            }
        }
    });
}))();
setTimeout(() => Publisher.publish('Price Server', JSON.stringify(['start', 'alpaca'])), 1000);
//setTimeout(()=>Publisher.publish('Price Server', JSON.stringify(['sub-trades','alpaca','NVDA'])),10000)
//setTimeout(()=>Publisher.publish('Price Server', JSON.stringify(['sub-trades','alpaca','SSS'])),10000)
