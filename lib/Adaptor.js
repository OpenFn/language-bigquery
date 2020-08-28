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

var _languageCommon = require("language-common");

var _cheerio = _interopRequireDefault(require("cheerio"));

var _cheerioTableparser = _interopRequireDefault(require("cheerio-tableparser"));

var _fs = _interopRequireDefault(require("fs"));

var _csvParse = _interopRequireDefault(require("csv-parse"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { "default": obj }; }

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

function fetch(getRequest) {// make a get and unzip the response
}

function unzip(pathToCsv) {// something that unzips from a CSV and allows the output to be used for hte
  // input of `load(data, options)`
}

function load(data, options) {// something that loads data (from a CSV?) to BigQuery
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
