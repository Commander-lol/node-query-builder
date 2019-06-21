/*
 * Copyright (C) LaunchBase LTD - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Louis Capitanchik <louis.capitanchik@launchbase.solutions>, August 2018
 */

const QueryBuilder = require('../QueryBuilder')
const QB = QueryBuilder // Shorter statics

test('Can create a simple select with basic fields', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').select('id', 'name', 'email')

	expect(sql).toBe('SELECT "id", "name", "email" FROM "users"')
})

test('Will not generate DELETE without a condition', () => {
	const builder = new QueryBuilder()
	expect(() => builder.table('users').delete()).toThrow()
})

test('Generates a DELETE query where a field is not null', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').where('deleted_at', QB.Null(), 'IS NOT').delete()

	expect(sql).toBe('DELETE FROM "users" WHERE "deleted_at" IS NOT NULL')
})

test('Does not quote special "*" column in SELECT queries', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').select('*')

	expect(sql).toBe('SELECT * FROM "users"')
})

test('Replacements for WHERE clauses have matching keys both in the query and the replacements object', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').where('id', 'bar-baz').select('*')
	const replacements = builder.getReplacements()
	const replacementKeys = Object.keys(replacements)

	expect(sql).toBe(`SELECT * FROM "users" WHERE "id" = :${ replacementKeys[0] }`)
	expect(replacements[replacementKeys[0]]).toBe('bar-baz')
})

test('Multiple WHERE clauses are bracketed and joined', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').where('id', 'foo-bar-baz').where('deleted_at', QB.Null(), 'IS NOT').select('*')
	const replacements = Object.keys(builder.getReplacements())


	expect(sql).toBe(`SELECT * FROM "users" WHERE ("id" = :${ replacements[0] } AND "deleted_at" IS NOT NULL)`)
})

test('Fn correctly paramatises simple parameters', () => {
	const fn = QB.Fn('ST_DWITHIN', 'POINT(0.123, -50.312)', 'POINT(0.412, -49.911)', '5')
	const sql = fn.toSql()
	const replacements = fn.getReplacements()
	const replacementKeys = Object.keys(replacements)

	expect(replacementKeys.length).toBe(3)
	expect(sql).toBe(`ST_DWITHIN(:${ replacementKeys[0] }, :${ replacementKeys[1] }, :${ replacementKeys[2] })`)

	expect(replacements[replacementKeys[0]]).toBe('POINT(0.123, -50.312)')
	expect(replacements[replacementKeys[1]]).toBe('POINT(0.412, -49.911)')
	expect(replacements[replacementKeys[2]]).toBe('5')
})

test('Fn correctly nests Fn calls', () => {
	const fn1 = QB.Fn('ST_POINT', '0.123', '0.456')
	const fn2 = QB.Fn('ST_DISTANCE', 'POINT(0.666, 0.777)', fn1)

	const fn1Replacements = fn1.getReplacements()
	const fn1ReplacementKeys = Object.keys(fn1Replacements)

	const fn2Replacements = fn2.getReplacements()
	const fn2ReplacementKeys = Object.keys(fn2Replacements)

	const fn1Sql = fn1.toSql()
	const fn2Sql = fn2.toSql()

	expect(fn1Sql).toBe(`ST_POINT(:${ fn1ReplacementKeys[0] }, :${ fn1ReplacementKeys[1] })`)
	expect(fn2Sql).toBe(`ST_DISTANCE(:${ fn2ReplacementKeys[0] }, ${ fn1Sql })`)

	for (const key of fn1ReplacementKeys) {
		expect(fn2ReplacementKeys.includes(key)).toBe(true)
	}
})

test('Can use Fn as a complete WHERE clause', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('apartments').where(QB.Fn('model_is_available', 'foo', 'bar'), null, null).select('*')
	const replacements = builder.getReplacements()
	const replacementKeys = Object.keys(replacements)


	expect(sql).toBe(`SELECT * FROM "apartments" WHERE model_is_available(:${ replacementKeys[0] }, :${ replacementKeys[1] })`)
})

test('Raw clause does not get processed', () => {
	const builder = new QueryBuilder()
	const sql = builder.table('users').select(QB.Raw('\'foo\' as const_value, *'))

	expect(sql).toBe('SELECT \'foo\' as const_value, * FROM "users"')
})

test('Raw clause can contain multiple separate statements that get joined', () => {
	const builder = new QueryBuilder()
	const sql = builder.select(QB.Raw('EXISTS', QB.SubSelect(sub =>
		sub.table('price_data')
			.where('model_id', 'foo-bar-baz')
			.limit(1)
			.select('id')
	)))

	const replacements = builder.getReplacements()
	const replacementKeys = Object.keys(replacements)

	expect(replacementKeys.length).toBe(1)
	expect(replacements[replacementKeys[0]]).toBe('foo-bar-baz')

	expect(sql).toBe(`SELECT EXISTS (SELECT "id" FROM "price_data" WHERE "model_id" = :${ replacementKeys[0] } LIMIT 1)`)
})

test('Lateral cross joins allow selecting data with multiple names in a complex query', () => {
	const builder = new QueryBuilder()

	const durationValue = 'foo'
	const radiusValue = 'bar'
	const longValue = 'baz'
	const latValue = 'box'

	// eslint-disable-next-line no-unused-vars
	const sql = builder.table('apartments')
		.join(QB.LateralCrossJoin(QB.SubSelect(sub =>
			sub.select(QB.Case(
				'pricing',
				[
					QB.When(QB.Where(QB.Fn('model_has_price_data', QB.Column('apartments.id')), true), QB.SubSelect(whenHasPrice =>
						whenHasPrice.select(QB.Fn('get_model_pricing_for_duration', QB.Column('apartments.id'), durationValue))
					)),
					QB.When(QB.Where(QB.Fn('model_has_price_data', QB.Column('apartments.apartment_type_id')), true), QB.SubSelect(whenHasPrice =>
						whenHasPrice.select(QB.Fn('get_model_pricing_for_duration', QB.Column('apartments.apartment_type_id'), durationValue))
					)),
				],
				QB.Else(QB.SubSelect(elseCase => elseCase.select(QB.Cast(QB.Null(), 'INTEGER'))))
			))
		), null, 'pricelist'))
		.join(QB.Join('apartment_types', QB.Where('at.id', QB.Column('apartments.apartment_type_id')), 'at'))
		.join(QB.Join('properties', QB.Where('pr.id', QB.Column('at.property_id')), 'pr'))
		.where(QB.Fn('ST_DWITHIN', QB.Column('pr.location'), QB.Fn('ST_POINT', longValue, latValue), radiusValue), null, null)
		.where('pricing', 4000, '<')
		.order(QB.Fn('ST_DISTANCE', QB.Column('pr.location'), QB.Fn('ST_POINT', longValue, latValue)))
		.select('apartments.*', QB.Select('pricelist.pricing', 'pricing'))

	// Assert something
})
