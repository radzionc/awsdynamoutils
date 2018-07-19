const uuid = require('uuid/v4')
const AWS = require('aws-sdk')


const mergedParams = (...params) =>
  params.reduce(
    (acc, param) =>
      Object.entries(param).reduce((acc, [key, value]) => {
        acc[key] = Array.isArray(value)
          ? [...acc[key], ...value]
          : typeof value === 'object' ? { ...acc[key], ...value } : value
        return acc
      }, acc),
    {}
  )

const projectionExpression = attributes => {
  if (attributes === undefined) return {}
  const ProjectionExpression = attributes
    .map(attr => `${attr.includes('.') ? '' : '#'}${attr}, `)
    .reduce((acc, str) => acc + str)
    .slice(0, -2)

  const attributesToExpression = attributes.filter(attr => !attr.includes('.'))
  const ExpressionAttributeNames = attributesToExpression.reduce(
    (acc, attr) => {
      acc['#' + attr] = attr
      return acc
    },
    {}
  )

  return attributesToExpression.length
    ? { ProjectionExpression, ExpressionAttributeNames }
    : { ProjectionExpression }
}

const ProvisionedThroughput = {
  ReadCapacityUnits: 10,
  WriteCapacityUnits: 10
}

const getId = () =>
  'a' +
  uuid()
    .split('-')
    .join('')

const getModule = (config) => {
  const documentClient = new AWS.DynamoDB.DocumentClient(config)

  return ({
    tableParams: (
      tableName,
      keyName,
      keyType = 'S',
      provisionedThroughput = ProvisionedThroughput
    ) => ({
      TableName: tableName,
      KeySchema: [
        {
          AttributeName: keyName,
          KeyType: 'HASH'
        }
      ],
      AttributeDefinitions: [
        {
          AttributeName: keyName,
          AttributeType: keyType
        }
      ],
      ProvisionedThroughput: provisionedThroughput
    }),
    tableParamsWithCompositeKey: (
      tableName,
      hashName,
      rangeName,
      hashType = 'S',
      rangeType = 'S',
      provisionedThroughput = ProvisionedThroughput
    ) => ({
      TableName: tableName,
      KeySchema: [
        {
          AttributeName: hashName,
          KeyType: 'HASH'
        },
        {
          AttributeName: rangeName,
          KeyType: 'RANGE'
        }
      ],
      AttributeDefinitions: [
        {
          AttributeName: hashName,
          AttributeType: hashType
        },
        {
          AttributeName: rangeName,
          AttributeType: rangeType
        }
      ],
      ProvisionedThroughput: provisionedThroughput
    }),
    flatUpdateParams: params => ({
      UpdateExpression: `set ${Object.entries(params)
        .map(([key]) => `#${key} = :${key}, `)
        .reduce((acc, str) => acc + str)
        .slice(0, -2)}`,
      ExpressionAttributeNames: Object.keys(params).reduce(
        (acc, key) => ({
          ...acc,
          [`#${key}`]: key
        }),
        {}
      ),
      ExpressionAttributeValues: Object.entries(params).reduce(
        (acc, [key, value]) => ({
          ...acc,
          [`:${key}`]: value
        }),
        {}
      )
    }),
    searchByKeyParams: (key, value) => ({
      FilterExpression: '#a = :aa',
      ExpressionAttributeNames: {
        '#a': key
      },
      ExpressionAttributeValues: {
        ':aa': value
      }
    }),
    searchByPKParams: (key, value) => ({
      KeyConditionExpression: '#a = :aa',
      ExpressionAttributeNames: {
        '#a': key
      },
      ExpressionAttributeValues: {
        ':aa': value
      }
    }),
    getId,
    withId: item =>
      item.id
        ? item
        : {
            ...item,
            id: getId()
          },
    put: (TableName, Item) => documentClient.put({ TableName, Item }).promise(),
    mergedParams,
    projectionExpression,
    getByPK: (params, attributes = undefined) =>
      documentClient
        .get(
          mergedParams(params, attributes ? projectionExpression(attributes) : {})
        )
        .promise()
        .then(data => (Object.keys(data).length ? data.Item : undefined)),
    putToList: (params, listName, object) =>
      documentClient
        .update({
          ...params,
          UpdateExpression: 'set #listName = list_append(#listName, :newObject)',
          ExpressionAttributeValues: {
            ':newObject': [object]
          },
          ExpressionAttributeNames: {
            '#listName': listName
          }
        })
        .promise(),
    removeFromListByIndex: (params, listName, index) =>
      documentClient
        .update({
          ...params,
          UpdateExpression: `remove #listName[${index}]`,
          ExpressionAttributeNames: {
            '#listName': listName
          }
        })
        .promise(),
    setNewValue: (params, propName, value) =>
      documentClient
        .update({
          ...params,
          UpdateExpression: `set #value = :newValue`,
          ExpressionAttributeValues: {
            ':newValue': value
          },
          ExpressionAttributeNames: {
            '#value': propName
          }
        })
        .promise(),
    mergeInList: (params, listName, list) =>
      documentClient
        .update({
          ...params,
          UpdateExpression: `set #listName = list_append(#listName, :mergeList)`,
          ExpressionAttributeValues: {
            ':mergeList': list
          },
          ExpressionAttributeNames: {
            '#listName': listName
          }
        })
        .promise()
  })
}

module.exports = {
  ...getModule({}),
  getModule,
}
