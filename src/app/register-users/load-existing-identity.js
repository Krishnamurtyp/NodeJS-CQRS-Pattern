/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
/**
 * Queries for an existing set of credentials
 * @param {object} context The context into which we load the identity
 * @param {object} attributes Object containing the email we look up by
 * @param {string} attributes.email The email to look up by
 * @param {object} context.queries Object containing the query function we
 * need
 * @param {function} context.queries.byEmail Function to look up an
 * identity by email
 * @returns context A Promise resolving to the action context with the
 * existing identity (if any) loaded into it at `existingIdentity`
 */
function loadExistingIdentity (context) {
  return context.queries
    .byEmail(context.attributes.email)
    .then(existingIdentity => {
      context.existingIdentity = existingIdentity

      return context
    })
}

module.exports = loadExistingIdentity
