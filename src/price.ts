import { fork } from 'child_process';
import redis, { RedisClientType, createClient } from 'redis';
import 'dotenv/config';

const exchanges: any[] = [];
console.log(`redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`)
const Publisher: RedisClientType = createClient({
  url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
});

let Listener: RedisClientType = Publisher.duplicate();
Listener.on('error', (err) => console.log('Redis Listener Error', err));
Publisher.on('error', (err) => console.log('Redis Publisher Error', err));


console.clear()

const closeExchange = (exchange: any) => {
    for (let i = 0; i < exchanges.length; i++) {
        if (exchanges[i][0] == exchange) {
            exchanges[i][1].kill()
            exchanges.splice(i,1)
        }
    }
}

const openExchange = (exchange: string) => {
    let there = false

    for (let i = 0; i < exchanges.length; i++) {
        if (exchanges[i][0] == exchange) {
            there = true
        }
    }

    if (!there) {
        const exchangeProcess = fork(`./exchanges/${exchange}.js`)
        exchanges.push([exchange, exchangeProcess])

        exchangeProcess.on('exit', () => {
            console.log(`Exchange ${exchange} closed`)
            closeExchange(exchange)
            setTimeout(()=>openExchange(exchange), 5000)
        })

        exchangeProcess.on('message', (message:string[]) => {
            switch (message[1]) {
                case 'lost-connection' : {
                    console.log('Lost connection to ' + message[0] + ', reconnecting in 5 seconds.')
                break;
                }
                case 'open' : {
                    console.log(message[0] + ' connected successfully.')
                break;
                }
                case 'error' : {
                    console.log(message[0] + ' price error, reconnecting in 5 seconds.')
                break;
                }
                case 'sub-trades' : {
                    console.log("Subscribed to " + message[2] + ' trades. ' + exchange)
                break;
                }
                case 'unsub-trades' : {
                    console.log("Unsubscribed to " + message[2] + ' trades. ' + exchange)
                break
                }
                case 'sub-candles' : {
                    console.log("Subscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange)
                break
                }
                case 'already-subscribed' : {
                    console.log("Already subscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange)
                break
                }
                case 'unsub-candles' : {
                    console.log("Unsubscribed to " + message[2] + ' ' + message[3] + ' candles. ' + exchange)
                break
                }
            }
        })
    }
};

(async ()=>{
    
    await Publisher.connect()
    await Listener.connect()

    
    await Listener.subscribe('Price Server', (message) => {
        message = JSON.parse(message)
        switch (message[0]) {
            case 'start' : {
                openExchange(message[1])
                break;
            }
            case 'close' : {
                closeExchange(message[1])
                break;
            }
            case 'restart' : {
                closeExchange(message[1])
                openExchange(message[1])
                break;
            }
            case 'sub-trades':
            case 'unsub-trades' : {
                for (let i = 0; i < exchanges.length; i++) {
                    if (exchanges[i][0] == message[1]) {
                        exchanges[i][1].send([message[0], message[2]])
                    }
                }
                break;
            }
            case 'sub-candles' :
            case 'unsub-candles' : {
                for (let i = 0; i < exchanges.length; i++) {
                    if (exchanges[i][0] == message[1]) {
                        exchanges[i][1].send([message[0], message[2], message[3]])
                    }
                }
                break;
            }
        }
    })

})();

setTimeout(()=>Publisher.publish('Price Server', JSON.stringify(['start','alpaca'])),1000)
//setTimeout(()=>Publisher.publish('Price Server', JSON.stringify(['sub-trades','alpaca','NVDA'])),10000)
//setTimeout(()=>Publisher.publish('Price Server', JSON.stringify(['sub-trades','alpaca','SSS'])),10000)