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
 * @description Writes a RegistrationEmailSent event
 * @param {object} context The context
 * @param {object} context.messageStore A reference to the Message Store
 * @param {object} context.event The event we're handling
 * @return {Promise} A Promise that resolves to the context
 */
function writeRegistrationEmailSentEvent (context, err) {
  const event = context.event

  const registrationEmailSentEvent = {
    id: uuid(),
    type: 'RegistrationEmailSent',
    metadata: {
      traceId: event.metadata.traceId,
      userId: event.metadata.userId
    },
    data: {
      userId: context.identityId,
      emailId: event.data.emailId
    }
  }
  const identityStreamName = event.metadata.originStreamName

  return context.messageStore
    .write(identityStreamName, registrationEmailSentEvent)
    .then(() => context)
}

module.exports = writeRegistrationEmailSentEvent
