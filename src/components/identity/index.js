const Bluebird = require('bluebird')

const ensureNotRegistered = require('./ensure-not-registered')
const ensureRegistrationEmailNotSent = require('./ensure-registration-email-not-sent')
const loadIdentity = require('./load-identity')
const renderRegistrationEmail = require('./render-registration-email')
const writeRegistrationEmailSentEvent = require('./write-registration-email-sent-event')
const writeRegisteredEvent = require('./write-registered-event')
const writeSendCommand = require('./write-send-command')
const AlreadyRegisteredError = require('./already-registered-error')
const AlreadySentRegistrationEmailError = require('./already-sent-registration-email-error')

function createIdentityCommandHandlers ({ messageStore }) {
    return {
        Register: command => {
            const context = {
                messageStore: messageStore,
                command,
                identityId: command.data.userId
            }

            return Bluebird.resolve(context)
                .then(loadIdentity)
                .then(ensureNotRegistered)
                .then(writeRegisteredEvent)
                .catch(AlreadyRegisteredError, () => {})
        }
    }
}


function build({messageStore}){
    const identityCommandHandlers = createIdentityCommandHandlers({messageStore})
    const identityCommandSubscription = messageStore.createSubscription({
        streamName:'identity:command',
        handlers: identityCommandHandlers,
        subscriberId:'components:identity:command'
    })
    
    function start() {
        identityCommandSubscription.start()
    }
    
    return {
        identityCommandHandlers,
        start
    }
}

module.exports = build