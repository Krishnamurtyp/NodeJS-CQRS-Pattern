/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const uuid = require('uuid/v4')

/**
 * @description Writes a Registered event
 * @param {object} context The context
 * @param {object} context.messageStore A reference to the Message Store
 * @param {object} context.command The command we're handling
 * @return {Promise} A Promise that resolves to the context
 */
function writeRegisteredEvent (context, err) {
  const command = context.command

  const registeredEvent = {
    // ...
    id: uuid(),
    type: 'Registered',
    metadata: {
      traceId: command.metadata.traceId,
      userId: command.metadata.userId
    },
    data: {
      userId: command.data.userId,
      email: command.data.email,
      passwordHash: command.data.passwordHash
    }
  }
  const identityStreamName = `identity-${command.data.userId}`

  return context.messageStore
    .write(identityStreamName, registeredEvent)
    .then(() => context)
}

module.exports = writeRegisteredEvent
