const createKnexClient = require('./knex-client')
const createPostgresClient = require('./postgres-client')
const createMessageStore = require('./message-store')

const createHomeApp = require('./app/home')
const createRecordViewingsApp = require('./app/record-viewings')
const createRegisterUsersApp = require('./app/register-users')

const createHomePageAggregator = require('./aggregators/home-page')

function createConfig({ env }) {
    
    // Get knex client
    const knexClient = createKnexClient({
        connectionString: env.databaseUrl
    })
    
    // Get postgres client
    const postgresClient = createPostgresClient({
        connectionString: env.messageStoreConnectionString
    })
    
    // Get messageStore
    const messageStore = createMessageStore({db: postgresClient})


    const homeApp = createHomeApp({ db:knexClient })
    const recordViewingsApp = createRecordViewingsApp({ messageStore })
    const registerUsersApp = createRegisterUsersApp({
        db:knexClient,
        messageStore
    })

    const homePageAggregator = createHomePageAggregator({
        db:knexClient,
        messageStore
    })
    // Aggregators
    const aggregators = [homePageAggregator]
    
    // Components
    const components = []
    
    return {
        env,
        db: knexClient,
        homeApp,
        messageStore,
        recordViewingsApp,
        homePageAggregator,
        aggregators,
        components,
        registerUsersApp
    }
}

module.exports = createConfig
