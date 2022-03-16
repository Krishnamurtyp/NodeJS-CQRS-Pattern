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
 * @description Writes a `Register` command for the attributes that we
 * received
 * @param {object} context The action context
 * @param {object} context.attributes The user-supplied attributes
 * @param {object} context.attributes.email The email address
 * @param {string} context.traceId The traceId
 * @param {object} context.messageStore A reference to the Message Store
 * @param {string} context.passwordHash The hased password
 */
function writeRegisterCommand (context) {
  const userId = context.attributes.id
  const stream = `identity:command-${userId}`
  const command = { 
    id: uuid(),
    type: 'Register',
    metadata: {
      traceId: context.traceId,
      userId
    },
    data: {
      userId,
      email: context.attributes.email,
      passwordHash: context.passwordHash
    }
  }

  return context.messageStore.write(stream, command)
}

module.exports = writeRegisterCommand
