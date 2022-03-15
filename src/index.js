const createExpressApp = require('./app/express')
const createConfig = require('./config')
const env = require('./env')

const config = createConfig({env})
const app = createExpressApp({config, env})

function start() {
    // Config aggregators
    config.aggregators.forEach(aggregator => aggregator.start())
    
    // Config components
    config.components.forEach(component => component.start())

    // Listen port and print signal in console    
    app.listen(env.port, signalAppStart)
}

function signalAppStart() {
    console.log(`${env.appName} started`)
    console.table([['Port', env.port], ['Environment', env.env]])
}

module.exports ={
    app,
    config,
    start
}