import { getDocRef, setDb } from '@xiroex/firebase-admin'
import 'dotenv/config'
import { RedisClientType, createClient } from 'redis'

const Publisher: RedisClientType = createClient({
    url: `redis://:${process.env.REDIS_PASSWORD}@${process.env.REDIS_URL}`
})

Publisher.on('error', (err) => console.log('Redis Publisher Error', err))

async function main() {
    if (!Publisher.isOpen) await Publisher.connect()
    setDb('price')
    let snapshot:any
    snapshot = await getDocRef('exchanges', 'coinbase').get()
    console.log('We made it here!')
    console.log(snapshot.data())
    //More Soon
}

process && process.on('message', async (message:any) => {
    console.log(message)
    switch (message[0]) {
        case 'start' : {
            await main()
            break;
        }
        case 'sub-trades' : {
            //await addTrack(message[2], message[1])
            break;
        }
        case 'unsub-trades' : {
            //await removeTrack(message[2], message[1])
            break;
        }
        case 'sub-candles' : {
            //await addTrack(message[2], message[1])
            break;
        }
        case 'unsub-candles' : {
            //await removeTrack(message[2], message[1])
            break;
        }
    }
})