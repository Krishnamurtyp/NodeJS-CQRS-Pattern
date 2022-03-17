/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const AlreadySentRegistrationEmailError =
  require('./already-sent-registration-email-error')

/**
 * @description Checks the identity entity to see if this identity's
 * registration email has already been sent
 * @param {object} context The context this is happening in
 * @param {object} context.identity The projected identity
 * @param {bool} context.identity.registrationEmailSent If the email has been
 * sent
 * @throws {AlreadySentRegistrationEmailError} If the email was already sent
 */
function ensureRegistrationEmailNotSent (context) {
  if (context.identity.registrationEmailSent) {
    throw new AlreadySentRegistrationEmailError()
  }

  return context
}

module.exports = ensureRegistrationEmailNotSent
