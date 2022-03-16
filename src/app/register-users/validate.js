/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const validate = require('validate.js')

const ValidationError = require('../errors/validation-error')

const constraints = { 
  email: {
    email: true,
    presence: true
  },
  password: {
    length: { minimum: 8 },
    presence: true
  }
}

/**
 * @description Validates the email and password a user is attempting
 * to register with
 * @param {object} context The context for the promise chain
 * @param {object} context.attributes The attributes to validate
 * @param {object} context.attributes.email The email to validate
 * @param {object} context.attributes.password The password to validate
 * @returns {object} The new action context
 */
function v (context) {
  const validationErrors = validate(context.attributes, constraints) 

  if (validationErrors) {
    throw new ValidationError(validationErrors)
  }

  return context
}

module.exports = v
