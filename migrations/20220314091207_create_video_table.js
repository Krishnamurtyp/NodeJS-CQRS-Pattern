/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function(knex) {
  return knex.schema.createTable('videos', table =>{
      table.increments()
      table.string('owner_id')
      table.string('name')
      table.string('description')
      table.string('transcoding_status')
      table.integer('view_count').defaultTo(0)
  })
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function(knex) {
  return knex.schema.dropTable('videos')
};
