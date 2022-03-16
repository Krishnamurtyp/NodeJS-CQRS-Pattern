/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const ValidationError = require('../errors/validation-error')

/**
 * Checks to see if `context` has an `existingIdentity` on it
 * @param {object} context The context into which we load the identity
 * @param {object} context.existingIdentity The existing identity
 * @returns {Promise<object>} A Promise resolving to the action context with the
 * existing identity (if any) loaded into it
 * @throws {ValidationError}
 */
function ensureThereWasNoExistingIdentity (context) {
  if (context.existingIdentity) {
    throw new ValidationError({ email: ['already taken'] })
  }

  return context
}

module.exports = ensureThereWasNoExistingIdentity
