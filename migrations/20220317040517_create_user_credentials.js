/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function(knex) {
    return knex.schema.createTable('user_credentials', table => {
        table.string('id').primary()
        table.string('email').notNullable()
        table.string('password_hash').notNullable()
        table.index('email')
    })
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function(knex) {
  return knex.schema.dropTable('user_credentials')
};
