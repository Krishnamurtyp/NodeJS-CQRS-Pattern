/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const bcrypt = require('bcrypt')

// We could pull this out into an environment variable, but we don't
const SALT_ROUNDS = 10

/**
 * @description Receives the context, hashes the supplied password, and
 * returns a new action context reflecting the hashed password
 * @param {object} context The action context
 * @param {object} context.attributes The user-supplied attributes
 * @param {string} context.attributes.password The password
 * @returns {Promise<object>} A Promise resolving to the context with a
 * `passwordHash` attribute added
 */
function hashPassword (context) {
  return bcrypt
    .hash(context.attributes.password, SALT_ROUNDS)
    .then(passwordHash => {
      context.passwordHash = passwordHash

      return context
    })
}

module.exports = hashPassword
