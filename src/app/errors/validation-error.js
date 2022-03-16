/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
function ValidationError (errors, options, attributes, constraints) {
  Error.captureStackTrace(this, this.constructor)
  this.message = `Validation error**${JSON.stringify(errors)}`
  this.errors = errors
  this.options = options
  this.attributes = attributes
  this.constraints = constraints
  this.name = 'ValidationError'
}

ValidationError.prototype = Object.create(Error.prototype)
ValidationError.prototype.constructor = ValidationError

module.exports = ValidationError
