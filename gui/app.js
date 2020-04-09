const express = require('express')
const app = express()
const port = 3000

const redis = require('redis')
const client = redis.createClient(6379, 'redis');
const { promisify } = require('util');
const keysAsync = promisify(client.keys).bind(client);
const hgetallAsync = promisify(client.hgetall).bind(client);

app.set('view engine', 'ejs');

app.get('/', async (req, res) => {
    const keys = await keysAsync('stocks:name:*')
    const stocks = keys.map(stock => stock.split(':')[2]);

    res.render('stocks', { stocks })
})

app.get('/:symbol', async (req, res) => {
    const { params: { symbol } } = req

    const model = await hgetallAsync(`stocks:query:${symbol}`)

    let dates = []
    let value = []
    for (let [key, point] of Object.entries(model)) {
        dates.push(key)
        value.push(point)
    }

    res.render('stock', { symbol, dates, value })
})

app.listen(port, () => console.log(`Example app listening at http://localhost:${port}`))
