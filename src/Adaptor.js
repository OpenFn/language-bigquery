/** @module Adaptor */
import { req, rawRequest } from './Client';
import { setAuth, setUrl } from './Utils';
import {
  execute as commonExecute,
  expandReferences,
  composeNextState,
} from 'language-common';
import cheerio from 'cheerio';
import cheerioTableparser from 'cheerio-tableparser';
import fs from 'fs';
import parse from 'csv-parse';

/**
 * Execute a sequence of operations.
 * Wraps `language-common/execute`, and prepends initial state for http.
 * @example
 * execute(
 *   create('foo'),
 *   delete('bar')
 * )(state)
 * @function
 * @param {Operations} operations - Operations to be performed.
 * @returns {Operation}
 */
export function execute(...operations) {
  const initialState = {
    references: [],
    data: null,
  };

  return state => {
    return commonExecute(...operations)({ ...initialState, ...state });
  };
}

/**
 * Make a GET request
 * @public
 * @example
 *  get("/myendpoint", {
 *      query: {foo: "bar", a: 1},
 *      headers: {"content-type": "application/json"},
 *      authentication: {username: "user", password: "pass"}
 *    },
 *    function(state) {
 *      return state;
 *    }
 *  )
 * @function
 * @param {string} path - Path to resource
 * @param {object} params - Query, Headers and Authentication parameters
 * @param {function} callback - (Optional) Callback function
 * @returns {Operation}
 */
export function get(path, params, callback) {
  return state => {
    const url = setUrl(state.configuration, path);

    const {
      query,
      headers,
      authentication,
      body,
      formData,
      options,
      ...rest
    } = expandReferences(params)(state);

    const auth = setAuth(state.configuration, authentication);

    return req('GET', { url, query, auth, headers, rest }).then(response => {
      const nextState = composeNextState(state, response);
      if (callback) return callback(nextState);
      return nextState;
    });
  };
}

export function fetch(getRequest) {
  // make a get and unzip the response
}

export function unzip(pathToCsv) {
  // something that unzips from a CSV and allows the output to be used for hte
  // input of `load(data, options)`
}

export function load(data, options) {
  // something that loads data (from a CSV?) to BigQuery
}

/**
 * CSV-Parse for CSV conversion to JSON
 * @public
 * @example
 *  parseCSV("/home/user/someData.csv", {
 * 	  quoteChar: '"',
 * 	  header: false,
 * 	});
 * @function
 * @param {String} target - string or local file with CSV data
 * @param {Object} config - csv-parse config object
 * @returns {Operation}
 */
export function parseCSV(target, config) {
  return state => {
    return new Promise((resolve, reject) => {
      var csvData = [];

      try {
        fs.readFileSync(target);
        fs.createReadStream(target)
          .pipe(parse(config))
          .on('data', csvrow => {
            console.log(csvrow);
            csvData.push(csvrow);
          })
          .on('end', () => {
            console.log(csvData);
            resolve(composeNextState(state, csvData));
          });
      } catch (err) {
        var csvString;
        if (typeof target === 'string') {
          csvString = target;
        } else {
          csvString = expandReferences(target)(state);
        }
        csvData = parse(csvString, config, (err, output) => {
          console.log(output);
          resolve(composeNextState(state, output));
        });
      }
    });
  };
}

export {
  alterState,
  dataPath,
  dataValue,
  each,
  field,
  fields,
  lastReferenceValue,
  merge,
  sourceValue,
} from 'language-common';
