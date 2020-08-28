"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.execute = execute;
exports.get = get;
exports.fetch = fetch;
exports.unzip = unzip;
exports.load = load;
exports.parseCSV = parseCSV;
Object.defineProperty(exports, "alterState", {
  enumerable: true,
  get: function get() {
    return _languageCommon.alterState;
  }
});
Object.defineProperty(exports, "dataPath", {
  enumerable: true,
  get: function get() {
    return _languageCommon.dataPath;
  }
});
Object.defineProperty(exports, "dataValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.dataValue;
  }
});
Object.defineProperty(exports, "each", {
  enumerable: true,
  get: function get() {
    return _languageCommon.each;
  }
});
Object.defineProperty(exports, "field", {
  enumerable: true,
  get: function get() {
    return _languageCommon.field;
  }
});
Object.defineProperty(exports, "fields", {
  enumerable: true,
  get: function get() {
    return _languageCommon.fields;
  }
});
Object.defineProperty(exports, "lastReferenceValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.lastReferenceValue;
  }
});
Object.defineProperty(exports, "merge", {
  enumerable: true,
  get: function get() {
    return _languageCommon.merge;
  }
});
Object.defineProperty(exports, "sourceValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.sourceValue;
  }
});

var _Client = require("./Client");

var _Utils = require("./Utils");

require("regenerator-runtime/runtime.js");

var _languageCommon = require("language-common");

var _cheerio = _interopRequireDefault(require("cheerio"));

var _cheerioTableparser = _interopRequireDefault(require("cheerio-tableparser"));

var _fs = _interopRequireDefault(require("fs"));

var _csvParse = _interopRequireDefault(require("csv-parse"));

var _admZip = _interopRequireDefault(require("adm-zip"));

var _request = _interopRequireDefault(require("request"));

var _bigquery = require("@google-cloud/bigquery");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { "default": obj }; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }

function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

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
function execute() {
  for (var _len = arguments.length, operations = new Array(_len), _key = 0; _key < _len; _key++) {
    operations[_key] = arguments[_key];
  }

  var initialState = {
    references: [],
    data: null
  };
  return function (state) {
    return _languageCommon.execute.apply(void 0, operations)(_objectSpread({}, initialState, {}, state));
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


function get(path, params, callback) {
  return function (state) {
    var url = (0, _Utils.setUrl)(state.configuration, path);

    var _expandReferences = (0, _languageCommon.expandReferences)(params)(state),
        query = _expandReferences.query,
        headers = _expandReferences.headers,
        authentication = _expandReferences.authentication,
        body = _expandReferences.body,
        formData = _expandReferences.formData,
        options = _expandReferences.options,
        rest = _objectWithoutProperties(_expandReferences, ["query", "headers", "authentication", "body", "formData", "options"]);

    var auth = (0, _Utils.setAuth)(state.configuration, authentication);
    return (0, _Client.req)('GET', {
      url: url,
      query: query,
      auth: auth,
      headers: headers,
      rest: rest
    }).then(function (response) {
      var nextState = (0, _languageCommon.composeNextState)(state, response);
      if (callback) return callback(nextState);
      return nextState;
    });
  };
}

function fetch(urlToFile) {
  return function (state) {
    return new Promise(function (resolve, reject) {
      (0, _request["default"])({
        url: urlToFile,
        method: 'GET',
        encoding: null
      }, function (err, res, body) {
        console.log(body);
        resolve(body); //state.data.body = body;
      });
    }).then(function (data) {
      return _objectSpread({}, state, {
        response: {
          body: data
        }
      });
    });
  }; // make a get and unzip the response
}

function unzip(pathToSave) {
  // something that unzips from a CSV and allows the output to be used for hte
  // input of `load(data, options)`
  return function (state) {
    var response = state.response;
    console.log(response.body);
    var zip = new _admZip["default"](response.body);
    var zipEntries = zip.getEntries();
    console.log(zipEntries.length);
    zip.extractAllTo(pathToSave, true);
    return state;
  };
}

function load(fileName, projectId, datasetId, tableId, loadOptions, callback) {
  // something that loads data (from a CSV?) to BigQuery
  return function (state) {
    var bigquery = new _bigquery.BigQuery({
      credentials: state.configuration,
      projectId: projectId
    }); // In this example, the existing table contains only the 'Name', 'Age',
    // & 'Weight' columns. 'REQUIRED' fields cannot  be added to an existing
    // schema, so the additional column must be 'NULLABLE'.

    function loadData() {
      return _loadData.apply(this, arguments);
    }

    function _loadData() {
      _loadData = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee() {
        var _yield$bigquery$datas, _yield$bigquery$datas2, table, destinationTableRef, options, _yield$bigquery$datas3, _yield$bigquery$datas4, job, errors;

        return regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return bigquery.dataset(datasetId).table(tableId).get();

              case 2:
                _yield$bigquery$datas = _context.sent;
                _yield$bigquery$datas2 = _slicedToArray(_yield$bigquery$datas, 1);
                table = _yield$bigquery$datas2[0];
                destinationTableRef = table.metadata.tableReference; // Set load job options

                options = _objectSpread({}, loadOptions, {
                  destinationTableRef: destinationTableRef
                }); // Load data from a local file into the table

                _context.next = 9;
                return bigquery.dataset(datasetId).table(tableId).load(fileName, options);

              case 9:
                _yield$bigquery$datas3 = _context.sent;
                _yield$bigquery$datas4 = _slicedToArray(_yield$bigquery$datas3, 1);
                job = _yield$bigquery$datas4[0];
                console.log("Job ".concat(job.id, " completed."));
                console.log('New Schema:');
                console.log(job.configuration.load.schema.fields); // Check the job's status for errors

                errors = job.status.errors;

                if (!(errors && errors.length > 0)) {
                  _context.next = 18;
                  break;
                }

                throw errors;

              case 18:
                return _context.abrupt("return", state);

              case 19:
              case "end":
                return _context.stop();
            }
          }
        }, _callee);
      }));
      return _loadData.apply(this, arguments);
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


function parseCSV(target, config) {
  return function (state) {
    return new Promise(function (resolve, reject) {
      var csvData = [];

      try {
        _fs["default"].readFileSync(target);

        _fs["default"].createReadStream(target).pipe((0, _csvParse["default"])(config)).on('data', function (csvrow) {
          console.log(csvrow);
          csvData.push(csvrow);
        }).on('end', function () {
          console.log(csvData);
          resolve((0, _languageCommon.composeNextState)(state, csvData));
        });
      } catch (err) {
        var csvString;

        if (typeof target === 'string') {
          csvString = target;
        } else {
          csvString = (0, _languageCommon.expandReferences)(target)(state);
        }

        csvData = (0, _csvParse["default"])(csvString, config, function (err, output) {
          console.log(output);
          resolve((0, _languageCommon.composeNextState)(state, output));
        });
      }
    });
  };
}

exports.fs = _fs["default"];
