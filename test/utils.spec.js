const {
  flatUpdateParams,
  mergedParams,
  projectionExpression
} = require('../src/utils')

describe('db utils', () => {
  test('flatUpdateParams', () => {
    const newProps = {
      one: true,
      two: 2
    }
    const { UpdateExpression, ExpressionAttributeValues } = flatUpdateParams(
      newProps
    )
    expect(UpdateExpression).toBe('set #one = :one, #two = :two')
    expect(ExpressionAttributeValues).toEqual({
      ':one': true,
      ':two': 2
    })
  })
  test('mergedParams', () => {
    const result = mergedParams(
      { table: 'ronni', first: { first: 'first' } },
      { second: { second: 'second' }, third: { lol: true } },
      { first: { second: 'second' }, second: { third: 'third' } }
    )
    const expected = {
      table: 'ronni',
      first: {
        first: 'first',
        second: 'second'
      },
      second: {
        second: 'second',
        third: 'third'
      },
      third: {
        lol: true
      }
    }

    expect(result).toEqual(expected)
  })
  test('projectionExpression', () => {
    const expected = {
      ProjectionExpression: '#name, #lol',
      ExpressionAttributeNames: {
        '#name': 'name',
        '#lol': 'lol'
      }
    }
    const result = projectionExpression(['name', 'lol'])
    expect(expected).toEqual(result)
  })
})
