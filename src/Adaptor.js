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
import AdmZip from 'adm-zip';
import request from 'request';

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

export function unzip() {
  return state => {
    const file_url =
      'https://github.com/mihaifm/linq/releases/download/3.1.1/linq.js-3.1.1.zip';

    return new Promise((resolve, reject) => {
      request({ url: file_url, method: 'GET' }, (err, res, body) => {
        //console.log(body);
        resolve(body);
        const zip = new AdmZip(body);
        const zipEntries = zip.getEntries();
        console.log(zipEntries.length);
      });
    });
    /* get(file_url, {}, () => {
      const zip = new AdmZip(body);
      const zipEntries = zip.getEntries();
      console.log(zipEntries.length);
    }); */

    // something that unzips from a CSV and allows the output to be used for hte
    // input of `load(data, options)`

    return state;
  };
}

export function load(
  fileName,
  datasetId,
  tableId,
  loadOptions,
  bQOptions,
  callback
) {
  // something that loads data (from a CSV?) to BigQuery
  return state => {
    const bigquery = new BigQuery(bQOptions);
    // In this example, the existing table contains only the 'Name', 'Age',
    // & 'Weight' columns. 'REQUIRED' fields cannot  be added to an existing
    // schema, so the additional column must be 'NULLABLE'.
    async function loadData() {
      // Retrieve destination table reference
      const [table] = await bigquery.dataset(datasetId).table(tableId).get();

      const destinationTableRef = table.metadata.tableReference;

      // Set load job options
      const options = { ...loadOptions, destinationTableRef };

      // Load data from a local file into the table
      const [job] = await bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(fileName, options);

      console.log(`Job ${job.id} completed.`);
      console.log('New Schema:');
      console.log(job.configuration.load.schema.fields);

      // Check the job's status for errors
      const errors = job.status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }

      console.log('hello?');
      return state;
    }
    return loadData();
  };
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
