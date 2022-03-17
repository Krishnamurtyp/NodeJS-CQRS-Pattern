/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const AlreadyRegisteredError = require('./already-registered-error')

/**
 * @description Checks the projected stream to see if the identity is already
 * registered
 * @param {object} context The context this is happening in
 * @param {object} context.identity The projected identity
 * @param {bool} context.identity.isRegistered If the identity is registered
 * @throws {AlreadyRegisteredError} If the identity is was already registered
 */
function ensureNotRegistered (context) {
  if (context.identity.isRegistered) {
    throw new AlreadyRegisteredError()
  }

  return context
}

module.exports = ensureNotRegistered
