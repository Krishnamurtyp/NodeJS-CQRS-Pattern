/**
 * @param {import("express").Express} app
 * @param { import("../../config.js") } config
 */
function mountRoutes(app, config){
    app.use('/', config.homeApp.router);
    app.use('/record-viewing', config.recordViewingsApp.router)
    app.use('/register', config.registerUsersApp.router)
}

module.exports = mountRoutes;