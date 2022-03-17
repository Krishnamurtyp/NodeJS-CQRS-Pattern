/***
 * Excerpted from "Practical Microservices",
 * published by The Pragmatic Bookshelf.
 * Copyrights apply to this code. It may not be used to create training material,
 * courses, books, articles, and the like. Contact us if you are in doubt.
 * We make no guarantees that this code is fit for any purpose.
 * Visit http://www.pragmaticprogrammer.com/titles/egmicro for more book information.
***/
const Email = require('email-templates')
const { join } = require('path')

const templateRoot = join(__dirname, 'templates')

/**
 * @description Renders the registration email for an identity and attaches it
 * to context.email
 * @param {object} context
 * @param {object} context.identity The identity we're rendering this for
 * @return {Promise} A Promise resolving to the context
 */
function renderEmail (context) {
  const email = new Email({ views: { root: templateRoot } })

  return email.renderAll('registration-email', {})
    .then(rendered => {
      context.email = rendered

      return context
    })
}

module.exports = renderEmail
