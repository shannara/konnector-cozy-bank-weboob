const {
  requestFactory,
  updateOrCreate,
  log,
  errors,
  categorize,
  cozyClient
} = require('cozy-konnector-libs')
const groupBy = require('lodash/groupBy')
const omit = require('lodash/omit')
const moment = require('moment')
const cheerio = require('cheerio')

const helpers = require('./helpers')

const doctypes = require('cozy-doctypes')
const {
  Document,
  BankAccount,
  BankTransaction,
  BalanceHistory,
  BankingReconciliator
} = doctypes

let baseUrl = 'http://localhost:5002/'
let urlLogin = ''

BankAccount.registerClient(cozyClient)
BalanceHistory.registerClient(cozyClient)
Document.registerClient(cozyClient)

const reconciliator = new BankingReconciliator({ BankAccount, BankTransaction })
const request = requestFactory({
  cheerio: true,
  json: false,
  jar: true
})

let lib

/**
 * The start function is run by the BaseKonnector instance only when it got all the account
 * information (fields). When you run this connector yourself in "standalone" mode or "dev" mode,
 * the account information come from ./konnector-dev-config.json file
 * @param {object} fields
 */
async function start(fields) {
  log('info', 'Build urls')
  log('info', baseUrl, 'Base url')

  urlLogin = baseUrl + 'auth'

  // ---

  log('info', 'Authenticating ...')
  let is_auth = await authenticate(fields.login, fields.password)
  if (!is_auth) {
    throw new Error(errors.LOGIN_FAILED)
  }
  log('info', 'Successfully logged in')

  log('info','Retrieve the JSON containing the list of bank accounts')
  let accounts_list = await downloadJsonWithBankInformation()

  log('info', 'Parsing list of bank accounts')
  const bankAccounts = await lib.parseBankAccounts(accounts_list)

  log('info', 'Parsing list of transactions by bank account')
  let allOperations = []

  allOperations = await lib.parseOperations(bankAccounts)

  /*log('info', 'Categorize the list of transactions')
  const categorizedTransactions = await categorize(JSON.stringify(allOperations))

  const { accounts: savedAccounts } = await reconciliator.save(
    bankAccounts.map(x => omit(x, ['currency'])),
    categorizedTransactions
  )*/
  const { accounts: savedAccounts } = await reconciliator.save(
    bankAccounts.map(x => omit(x, ['currency'])),
    allOperations
  )

  log(
    'info',
    'Retrieve the balance histories and adds the balance of the day for each bank accounts'
  )
  const balances = await fetchBalances(savedAccounts)

  log('info', 'Save the balance histories')
  await lib.saveBalances(balances)
}

// ============

/**
 * This function initiates a connection on the CreditMutuel website.
 *
 * @param {string} user
 * @param {string} password
 * @returns {boolean} Returns true if authentication is successful, else false
 * @throws {Error} When the website is down or an HTTP error has occurred
 */
function authenticate(user, password) {
  return request({
    uri: urlLogin,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    // HACK: Form option doesn't correctly encode values.
    body:
      JSON.stringify({'username': 'user', 'password': 'pass'}),
    transform: (body, response) => [
      response.statusCode,
      cheerio.load(body),
      response
    ]
  })
    .then(([statusCode, $, fullResponse]) => {
      // Need tweak authent part
      /*if (fullResponse.request.uri.href === urlLogin) {
        log(
          'error',
          statusCode + ' ' + $('.blocmsg.err').text(),
          errors.LOGIN_FAILED
        )
        return false
      }*/
      return true
    })
    .catch(err => {
      if (err.statusCode >= 500) {
        throw new Error(errors.VENDOR_DOWN)
      } else {
        log('error', errors.LOGIN_FAILED, err.statusCode)
        throw new Error(errors.LOGIN_FAILED)
      }
    })
}

/**
 * Downloads an JSON file containing all bank accounts.
 *
 * @returns {json} JSON downloaded from weboobapi.
 * It contains all bank accounts.
 */
async function downloadJsonWithBankInformation() {
  return request({
    uri: baseUrl + 'cap',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: {
      capability: 'bank',
      command: 'list'
    },
    json: true,
    transform: (body, response) => {
       if (response.headers['content-type'] === 'application/json') {
           return JSON.parse(body);
       } else {
         log('error', body)
         return body;
       }
     }
  })
}

/**
 * Downloads an JSON file containing all operations of bank accounts.
 *
 * @returns {json} JSON downloaded from weboobapi.
 * It contains all operations of bank accounts.
 */
async function downloadJsonWithOperationsList(accountId) {
  fullAccountId = accountId + "@creditmutuel"
  return request({
    uri: baseUrl + 'cap',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: {
      capability: 'bank',
      command: 'history',
      args: fullAccountId
    },
    json: true,
    transform: (body, response) => [
      response.statusCode,
      body,
      response
    ]
  })
  .then(([statusCode, body, fullResponse]) => {
    return body;
  })
}

/**
 * Parses and transforms each lines (JSON format) into
 * {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankaccounts|io.cozy.bank.accounts}
 * @param {array} bankAccountLines Lines containing the bank account information - JSON format expected
 * @example
 * var bankAccountLines = [
    {
     "id": "xxx1@creditmutuel",
     "url": null,
     "label": "Compte Courant M X",
     "currency": "EUR",
     "bank_name": null,
     "type": 1,
     "owner_type": null,
     "balance": "999",
     ...
    }
   ];
 *
 * parseBankAccounts(json);
 *
 * // [
 * //   {
 * //     institutionLabel: 'CreditMutuel',
 * //     label: 'LIVRET',
 * //     type: 'Savings',
 * //     balance: 42,
 * //     number: 'XXXXXXXX',
 * //     vendorId: 'XXXXXXXX',
 * //     rawNumber: 'XXXXXXXX',
 * //     currency: 'EUR'
 * //   }
 * // ]
 *
 * @returns {array} Collection of
 * {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankaccounts|io.cozy.bank.accounts}
 */
 const parseBankAccount = bankAccount => {
   return {
     institutionLabel: 'CreditMutuel',
     label: bankAccount.label,
     type: helpers.parseLabelBankAccount(bankAccount.label),
     balance: helpers.normalizeAmount(bankAccount.balance),
     number: bankAccount.number,
     vendorId: bankAccount.number,
     rawNumber: bankAccount.number,
     currency: bankAccount.currency
   }
 }

function parseBankAccounts(bankAccountLines) {
  return bankAccountLines.map(parseBankAccount)
}

/**
 * Parses and transforms each lines (JSON format) into
 * {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankoperations|io.cozy.bank.operations}
 * @param {io.cozy.bank.accounts} account Bank account
 * @param {array} operationLines Lines containing operation information for the current bank account - JSON format expected
 *
 * @example
 * var account = {
 *    institutionLabel: 'CIC',
 *    label: 'LIVRET',
 *    type: 'Savings',
 *    balance: 42,
 *    number: 'XXXXXXXX',
 *    vendorId: 'XXXXXXXX',
 *    rawNumber: 'XXXXXXXX',
 *    currency: 'EUR'
 * };
 *
 * var operationLines = [
   {
     "id": "@creditmutuel",
     "date": "2020-04-02",
     "rdate": "2020-04-02",
     "vdate": "2020-04-02",
     ...,
     "label": "VIR SEPA LOCATION BOX", "amount": "-89.00",
     ...
    }
 * ];
 *
 * parseOperations(account, json);
 * // [
 * //   {
 * //     label: 'INTERETS 2018',
 * //     type: 'direct debit',
 * //     cozyCategoryId: '200130',
 * //     cozyCategoryProba: 1,
 * //     date: "2018-12-30T23:00:00+01:00",
 * //     dateOperation: "2018-12-31T23:00:00+01:00",
 * //     dateImport: "2019-04-17T10:07:30.553Z",       (UTC)
 * //     currency: 'EUR',
 * //     vendorAccountId: 'XXXXXXXX',
 * //     amount: 38.67,
 * //     vendorId: 'XXXXXXXX_2018-12-30_0'             {number}_{date}_{index}
 * //   }
 *
 * @returns {array} Collection of {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankoperations|io.cozy.bank.operations}.
 */
async function parseOperationsPerAccount(account, operationLines) {
  const operations = operationLines.map(line => {
      let metadata = null
      let amount = 0
      //let date = helpers.parseDate(line.rdate)
      //let dateOperation = helpers.parseDate(line.vdate)
      amount = helpers.normalizeAmount(line.amount)
      metadata = helpers.findMetadataForDebitOperation(line.label)

      return {
        label: line.label,
        type: metadata._type || 'none',
        cozyCategoryId: 0,
        cozyCategoryProba: 1,
        date: moment(line.rdate).format(),
        dateOperation: moment(line.vdate).format(),
        dateImport: new Date().toISOString(),
        currency: account.currency,
        vendorAccountId: account.vendorId,
        amount: amount
      }
  })

  // Forge a vendorId by concatenating account number, day YYYY-MM-DD and index
  // of the operation during the day
  const groups = groupBy(operations, x => x.date.slice(0, 10))
  Object.entries(groups).forEach(([date, group]) => {
    group.forEach((operation, i) => {
      operation.vendorId = account.vendorId + '_' + date + '_' + i
    })
  })

  return operations
}

const flatten = iterables => {
  const res = []
  for (const iterable of iterables) {
    for (const item of iterable) {
      res.push(item)
    }
  }
  return res
}

async function parseOperations(accounts) {
  const operationsPerAccount = await Promise.all(
    accounts.map(async account => {
      let operations_list = await downloadJsonWithOperationsList(account.rawNumber)
      return parseOperationsPerAccount(account, JSON.parse(operations_list))
    })
  )
  return flatten(operationsPerAccount)
}

/**
 * Retrieves the balance history for one year and an account. If no balance history is found,
 * this function returns an empty document based on {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankbalancehistories|io.cozy.bank.balancehistories} doctype.
 * <br><br>
 * Note: Can't use <code>BalanceHistory.getByYearAndAccount()</code> directly for the moment,
 * because <code>BalanceHistory</code> invokes <code>Document</code> that doesn't have an cozyClient instance.
 *
 * @param {integer} year
 * @param {string} accountId
 * @returns {io.cozy.bank.balancehistories} The balance history for one year and an account.
 */
async function getBalanceHistory(year, accountId) {
  const index = await BalanceHistory.getIndex(
    BalanceHistory.doctype,
    BalanceHistory.idAttributes
  )
  const options = {
    selector: { year, 'relationships.account.data._id': accountId },
    limit: 1
  }
  const [balance] = await BalanceHistory.query(index, options)

  if (balance) {
    return balance
  }

  return BalanceHistory.getEmptyDocument(year, accountId)
}

/**
 * Retrieves the balance histories of each bank accounts and adds the balance of the day for each bank account.
 * @param {array} accounts Collection of {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankaccounts|io.cozy.bank.accounts}
 * already registered in database
 *
 * @example
 * var accounts = [
 *    {
 *      _id: '12345...',
 *      _rev: '14-98765...',
 *      _type: 'io.cozy.bank.accounts',
 *      balance: 42,
 *      cozyMetadata: { updatedAt: '2019-04-17T10:07:30.769Z' },
 *      institutionLabel: 'CIC',
 *      label: 'LIVRET',
 *      number: 'XXXXXXXX',
 *      rawNumber: 'XXXXXXXX',
 *      type: 'Savings',
 *      vendorId: 'XXXXXXXX'
 *    }
 * ];
 *
 *
 * fetchBalances(accounts);
 *
 * // [
 * //   {
 * //     _id: '12345...',
 * //     _rev: '9-98765...',
 * //     balances: { '2019-04-16': 42, '2019-04-17': 42 },
 * //     metadata: { version: 1 },
 * //     relationships: { account: [Object] },
 * //     year: 2019
 * //   }
 * // ]
 *
 * @returns {array} Collection of {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankbalancehistories|io.cozy.bank.balancehistories}
 * registered in database
 */
function fetchBalances(accounts) {
  const now = moment()
  const todayAsString = now.format('YYYY-MM-DD')
  const currentYear = now.year()

  return Promise.all(
    accounts.map(async account => {
      const history = await getBalanceHistory(currentYear, account._id)
      history.balances[todayAsString] = account.balance

      return history
    })
  )
}

/**
 * Saves the balance histories in database.
 *
 * @param balances Collection of {@link https://docs.cozy.io/en/cozy-doctypes/docs/io.cozy.bank/#iocozybankbalancehistories|io.cozy.bank.balancehistories}
 * to save in database
 * @returns {Promise}
 */
function saveBalances(balances) {
  return updateOrCreate(balances, 'io.cozy.bank.balancehistories', ['_id'])
}

// ===== Export ======

String.prototype.replaceAll = function(search, replacement) {
  var target = this
  return target.replace(new RegExp(search, 'g'), replacement)
}

module.exports = lib = {
  start,
  authenticate,
  parseBankAccounts,
  parseOperations,
  fetchBalances,
  saveBalances
}
